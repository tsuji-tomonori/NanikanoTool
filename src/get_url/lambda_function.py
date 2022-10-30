from __future__ import annotations

import os
import json
import time
import logging
from typing import Any, NamedTuple

import requests
import boto3
from bs4 import BeautifulSoup


logger = logging.getLogger()


class EnvironParams(NamedTuple):
    LOG_LEVEL: str
    URL_QUEUE_URL: str
    IMG_QUEUE_URL: str
    PKEY: str
    URL_PARAM: str
    DB_NAME: str
    TOPICK_ARN: str
    N_RETRY: str

    @classmethod
    def from_env(cls) -> EnvironParams:
        return EnvironParams(**{k: os.environ[k] for k in EnvironParams._fields})


class AwsBase:
    def __init__(self, profile: str | None = None) -> None:
        self.session = boto3.Session(profile_name=profile)


class Ssm(AwsBase):
    def get_paramater(self, key: str) -> str:
        client = self.session.client("ssm")
        value = client.get_parameter(
            Name=key,
            WithDecryption=True
        )
        return value["Parameter"]["Value"]


class DynamoDb(AwsBase):
    def put(self, db_name: str, pkey: str, url: str) -> dict:
        dynamodb = self.session.resource("dynamodb")
        table = dynamodb.Table(db_name)
        table.put_item(
            Item={pkey: url}
        )


class Sns(AwsBase):
    def publish(self, topick_arn: str, message: str, subject: str) -> None:
        client = self.session.client("sns")
        client.publish(
            TopicArn=topick_arn,
            Message=message,
            Subject=subject,
        )


class Sqs(AwsBase):
    def send(self, queue_url: str, message: dict) -> None:
        client = self.session.client("sqs")
        client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
            DelaySeconds=10,
        )


class Dependencies(NamedTuple):
    ssm: Ssm
    dynamodb: DynamoDb
    sns: Sns
    sqs: Sqs

    @classmethod
    def on_lambda(cls) -> Dependencies:
        return Dependencies(
            ssm=Ssm(),
            dynamodb=DynamoDb(),
            sns=Sns(),
            sqs=Sqs(),
        )

    @classmethod
    def on_local(cls, profile: str) -> Dependencies:
        return Dependencies(
            ssm=Ssm(profile),
            dynamodb=DynamoDb(profile),
            sns=Sns(profile),
            sqs=Sqs(profile),
        )


class ServiceParam(NamedTuple):
    archive_number: int
    archive_url: str

    @classmethod
    def of(cls, archive_url: str) -> ServiceParam:
        return ServiceParam(
            archive_number=archive_url.split("/")[-1],
            archive_url=archive_url,
        )


def index_urls(base_url: str) -> list[str]:
    return [base_url] + [f"{base_url}page/{i}" for i in range(2, 191+1)]


def archives_urls(index_url: str) -> list[str]:
    response = requests.get(index_url)
    soup = BeautifulSoup(response.text, 'lxml')
    return [l.get("href") for l in soup.find_all(class_="post-link")]


def service(body: dict[str, Any], ep: EnvironParams, dp: Dependencies) -> None:
    logger.info("control start")
    idx = body.get("index", 0)
    urls = index_urls(dp.ssm.get_paramater(ep.URL_PARAM))
    for archive_url in archives_urls(urls[idx]):
        dp.dynamodb.put(ep.DB_NAME, ep.PKEY, archive_url)
    next_idx = idx + 1
    if next_idx > 191:
        dp.sns.publish(ep.TOPICK_ARN, "処理が完了しました", "DB登録通知")
        dp.sqs.send(ep.IMG_QUEUE_URL, {"start": "get_url lambda"})
    else:
        dp.sqs.send(ep.URL_QUEUE_URL, {"index": next_idx})


def control(record: dict[str, Any], ep: EnvironParams, dp: Dependencies) -> None:
    logger.info("control_loop start")
    # SQSの中身をパース
    body = json.loads(record["body"])
    # リトライオーバー
    if body.get("retry", 0) > int(ep.N_RETRY):
        logger.error("[RetryOver]")
        return
    # try:
    service(body, ep, dp)
    # except:
    #     logger.exception()
    #     # 例外発生時はeventの内容を投入
    #     body["retry"] = body.get("retry", 0) + 1
    #     dp.sqs.send(ep.TOPICK_ARN, body)
    #     logger.warning("Retry Start")


def lambda_handler(event, context) -> int:
    ep = EnvironParams.from_env()
    logger.setLevel(ep.LOG_LEVEL)
    logger.info(json.dumps(event, indent=2))
    dp = Dependencies.on_lambda()
    for ix, record in enumerate(event["Records"]):
        control(record, ep, dp)
        if ix != len(event["Records"]) - 1:
            logger.info("[wait][5s]")
            time.sleep(5)
