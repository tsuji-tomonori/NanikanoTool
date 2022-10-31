from __future__ import annotations

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_ssm as ssm,
    aws_lambda_event_sources as lambda_event,
    Duration,
    Tags,
)
from constructs import Construct


class SugokuWaruiTool(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        layer = lambda_.LayerVersion(
            self, "layer",
            code=lambda_.Code.from_asset(
                str(Path.cwd()),
                bundling=cdk.BundlingOptions(
                    image=cdk.DockerImage.from_build(
                        path=str(Path.cwd() / "layer"),
                    ),
                    command=[
                        "sh", "-c", "pip install -r layer/requirements.txt -t ../asset-output/python  --no-compile"]
                )
            ),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
        )

        saveimg_fn = lambda_.Function(
            self, "saveimg_fn",
            code=lambda_.Code.from_asset("src/save_img"),
            handler="lambda_function.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(30),
            environment=self.node.try_get_context("lambda_env"),
            memory_size=256,
            layers=[layer]
        )

        get_url_fn = lambda_.Function(
            self, "get_url_fn",
            code=lambda_.Code.from_asset("src/get_url"),
            handler="lambda_function.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(300),
            environment=self.node.try_get_context("lambda_env"),
            memory_size=256,
            layers=[layer]
        )

        saveimg_trigger = sqs.Queue(self, "saveimg_trigger")
        saveimg_fn.add_event_source(
            lambda_event.SqsEventSource(saveimg_trigger)
        )
        saveimg_trigger.grant_send_messages(saveimg_fn.role)
        saveimg_trigger.grant_send_messages(get_url_fn.role)
        saveimg_fn.add_environment("IMG_QUEUE_URL", saveimg_trigger.queue_url)
        get_url_fn.add_environment("IMG_QUEUE_URL", saveimg_trigger.queue_url)

        get_url_trigger = sqs.Queue(
            self, "get_url_trigger", visibility_timeout=Duration(300))
        get_url_fn.add_event_source(
            lambda_event.SqsEventSource(get_url_trigger)
        )
        get_url_trigger.grant_send_messages(get_url_fn.role)
        get_url_fn.add_environment("URL_QUEUE_URL", get_url_trigger.queue_url)

        bucket = s3.Bucket(self, "saveimg_bucket")
        bucket.grant_put(saveimg_fn.role)
        saveimg_fn.add_environment("BUCKET_NAME", bucket.bucket_name)

        db = dynamodb.Table(
            self, "archive",
            partition_key=dynamodb.Attribute(
                name=self.node.try_get_context("lambda_env")["PKEY"],
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            read_capacity=1,
            write_capacity=1,
        )
        db.grant_read_data(saveimg_fn.role)
        db.grant_write_data(get_url_fn.role)
        saveimg_fn.add_environment("DB_NAME", db.table_name)
        get_url_fn.add_environment("DB_NAME", db.table_name)

        fin_topic = sns.Topic(self, "fin_topic")
        fin_topic.grant_publish(saveimg_fn.role)
        fin_topic.grant_publish(get_url_fn.role)
        saveimg_fn.add_environment("TOPICK_ARN", fin_topic.topic_arn)
        get_url_fn.add_environment("TOPICK_ARN", fin_topic.topic_arn)

        param = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, self.node.try_get_context("URL_PARAM"),
            version=1,
            parameter_name=self.node.try_get_context("URL_PARAM"),
        )
        param.grant_read(get_url_fn.role)
        get_url_fn.add_environment("URL_PARAM", param.parameter_name)


app = cdk.App()
apigw_stack = SugokuWaruiTool(app, app.node.try_get_context("PROJECT_NAME"))
Tags.of(apigw_stack).add("Project", app.node.try_get_context("PROJECT_NAME"))
Tags.of(apigw_stack).add("Type", "Pro")
Tags.of(apigw_stack).add("Creator", "cdk")
app.synth()
