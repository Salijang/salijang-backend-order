import asyncio
import contextlib
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.future import select
from database import engine, SessionLocal
from routers import orders
from redis_client import restore_stock
from sqs_client import start_stock_result_consumer
import models


async def handle_stock_result(body: dict) -> None:
    event_type = body.get("event_type")
    if event_type != "stock_failed":
        return

    order_id = body.get("order_id")
    items = body.get("items", [])

    for item in items:
        await restore_stock(item["product_id"], item["quantity"])

    async with SessionLocal() as db:
        result = await db.execute(
            select(models.Order).filter(models.Order.id == order_id)
        )
        order = result.scalars().first()
        if order and order.status == "pending":
            order.status = "cancelled"
            await db.commit()
            print(f"[Saga] 주문 {order_id} 재고 차감 실패로 취소 처리")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_task = asyncio.create_task(start_stock_result_consumer(handle_stock_result))
    yield
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    await engine.dispose()


app = FastAPI(
    title="Sallijang Order Service",
    description="Microservice for managing pickup reservations and orders.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://sallijang.shop", "https://app.sallijang.shop"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(orders.router)


@app.get("/")
def read_root():
    return {"message": "Welcome to Sallijang Order Service API! Go to http://localhost:8002/docs to test endpoints."}


@app.get("/health")
def health():
    return {"status": "ok"}
