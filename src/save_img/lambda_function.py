from __future__ import annotations

import os
import json
from ssl import SSLError
import time
import logging
from typing import Any, NamedTuple

import requests
import boto3
from bs4 import BeautifulSoup


logger = logging.getLogger()


class EnvironParams(NamedTuple):
    LOG_LEVEL: str
    IMG_QUEUE_URL: str
    BUCKET_NAME: str
    DB_NAME: str
    TOPICK_ARN: str
    N_RETRY: str

    @classmethod
    def from_env(cls) -> EnvironParams:
        return EnvironParams(**{k: os.environ[k] for k in EnvironParams._fields})


class AwsBase:
    def __init__(self, profile: str | None = None) -> None:
        self.session = boto3.Session(profile_name=profile)


class S3(AwsBase):
    def upload(self, data: bytes, bucket_name: str, key: str) -> None:
        client = self.session.client("s3")
        client.put_object(
            Body=data,
            Bucket=bucket_name,
            Key=key,
        )


class DynamoDb(AwsBase):
    def scan(self, db_name: str, last_evaluated_key: str | None) -> dict:
        logger.info("DynamoDb start")
        client = self.session.client("dynamodb")
        param = {"TableName": db_name, "Limit": 1}
        if last_evaluated_key:
            param["ExclusiveStartKey"] = last_evaluated_key
        res = client.scan(**param)
        logger.info(f"dynamodb={json.dumps(res)}")
        if len(res["Items"]) > 1:
            raise Exception("想定以上にレコードを取得しています")
        return res


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
            DelaySeconds=60,
        )


class Dependencies(NamedTuple):
    s3: S3
    dynamodb: DynamoDb
    sns: Sns
    sqs: Sqs

    @classmethod
    def on_lambda(cls) -> Dependencies:
        return Dependencies(
            s3=S3(),
            dynamodb=DynamoDb(),
            sns=Sns(),
            sqs=Sqs(),
        )

    @classmethod
    def on_local(cls, profile: str) -> Dependencies:
        return Dependencies(
            s3=S3(profile),
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


def save_img(img: bytes, img_index: int, ep: EnvironParams, dp: Dependencies, sp: ServiceParam) -> None:
    dp.s3.upload(
        data=img,
        bucket_name=ep.BUCKET_NAME,
        key=f"{sp.archive_number}_{str(img_index).zfill(3)}.png",
    )


def get_img(img_url: str) -> bytes:
    return requests.get(img_url).content


def get_and_save_img(ep: EnvironParams, dp: Dependencies, sp: ServiceParam) -> None:
    logger.info("get_and_save_img start")
    response = requests.get(sp.archive_url)
    soup = BeautifulSoup(response.text, 'lxml')
    index = 0
    for obj in soup.find_all(class_="external"):
        img_url = obj.get("href")
        if ("jpg" in img_url) and ("naver" not in img_url):
            try:
                save_img(
                    img=get_img(img_url),
                    img_index=index,
                    ep=ep,
                    dp=dp,
                    sp=sp,
                )
            except SSLError:
                logger.exception()
                logger.info(f"[skip][{index}][{img_url}]")
            index += 1
            logger.info(f"[save][{index}][{img_url}]")
        else:
            logger.info(f"[skip][{index}][{img_url}]")


def control(body: dict[str, Any], ep: EnvironParams, dp: Dependencies) -> None:
    logger.info("control start")
    response = dp.dynamodb.scan(
        ep.DB_NAME, body.get("LastEvaluatedKey")
    )
    if len(response["Items"]) == 1:
        sp = ServiceParam.of(response["Items"][0]["archive_url"]["S"])
        get_and_save_img(ep=ep, dp=dp, sp=sp)
    if "LastEvaluatedKey" in response:
        last_evaluated_key = {"LastEvaluatedKey": response["LastEvaluatedKey"]}
        dp.sqs.send(ep.IMG_QUEUE_URL, last_evaluated_key)
    else:
        dp.sns.publish(ep.TOPICK_ARN, "処理が完了しました", "処理完了通知")


def control_loop(record: dict[str, Any], ep: EnvironParams, dp: Dependencies) -> None:
    logger.info("control_loop start")
    # SQSの中身をパース
    body = json.loads(record["body"])
    # リトライオーバー
    if body.get("retry", 0) > int(ep.N_RETRY):
        logger.error("[RetryOver]")
        return
    # try:
    control(body, ep, dp)
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
        control_loop(record, ep, dp)
        if ix != len(event["Records"]) - 1:
            logger.info("[wait][5s]")
            time.sleep(5)
