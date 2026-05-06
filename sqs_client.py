import asyncio
import boto3
import json
import os
from typing import Callable, Awaitable
from botocore.config import Config

SQS_QUEUE_URL         = os.getenv("SQS_QUEUE_URL", "")
STOCK_DEDUCT_QUEUE_URL = os.getenv("STOCK_DEDUCT_QUEUE_URL", "")
STOCK_RESULT_QUEUE_URL = os.getenv("STOCK_RESULT_QUEUE_URL", "")
AWS_REGION            = os.getenv("AWS_REGION", "ap-northeast-2")

_BOTO_CONFIG = Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 2})
_sqs_client = None


def _get_sqs():
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client("sqs", region_name=AWS_REGION, config=_BOTO_CONFIG)
    return _sqs_client


async def publish_order_event(payload: dict) -> None:
    if not SQS_QUEUE_URL:
        return
    def _send():
        _get_sqs().send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(payload))
    try:
        await asyncio.to_thread(_send)
    except Exception as e:
        print(f"[SQS] publish_order_event 실패: {e}")


async def publish_stock_deduct_event(payload: dict) -> None:
    if not STOCK_DEDUCT_QUEUE_URL:
        return
    def _send():
        _get_sqs().send_message(QueueUrl=STOCK_DEDUCT_QUEUE_URL, MessageBody=json.dumps(payload))
    try:
        await asyncio.to_thread(_send)
    except Exception as e:
        print(f"[SQS] publish_stock_deduct_event 실패: {e}")


async def start_stock_result_consumer(process_fn: Callable[[dict], Awaitable[None]]) -> None:
    if not STOCK_RESULT_QUEUE_URL:
        print("[SQS StockResult] STOCK_RESULT_QUEUE_URL 미설정 — 비활성화")
        return

    def _receive():
        return _get_sqs().receive_message(
            QueueUrl=STOCK_RESULT_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
        )

    def _delete(receipt_handle: str):
        _get_sqs().delete_message(QueueUrl=STOCK_RESULT_QUEUE_URL, ReceiptHandle=receipt_handle)

    print("[SQS StockResult] 소비자 시작")
    while True:
        try:
            response = await asyncio.to_thread(_receive)
            for msg in response.get("Messages", []):
                try:
                    body = json.loads(msg["Body"])
                    await process_fn(body)
                    await asyncio.to_thread(_delete, msg["ReceiptHandle"])
                except Exception as e:
                    print(f"[SQS StockResult] 메시지 처리 실패: {e}")
        except asyncio.CancelledError:
            print("[SQS StockResult] 종료")
            return
        except Exception as e:
            print(f"[SQS StockResult] 오류: {e}")
            await asyncio.sleep(5)
