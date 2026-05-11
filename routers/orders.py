import asyncio
import json
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import func, cast
from sqlalchemy.types import Date as SQLDate
from typing import List, Optional
import datetime
import os
import time
import uuid
import httpx

from database import get_db
from deps import get_current_user, CurrentUser
from redis_client import reserve_stock, restore_stock, get_redis
from sqs_client import publish_order_event, publish_stock_deduct_event
import models
import schemas

router = APIRouter(prefix="/api/v1/orders", tags=["Orders"])

PRODUCT_SERVICE_URL = os.getenv("PRODUCT_SERVICE_URL", "http://localhost:8001")
ORDER_SLOW_LOG_MS = int(os.getenv("ORDER_SLOW_LOG_MS", "1000"))


class StepTimer:
    def __init__(self, name: str):
        self.name = name
        self.start = time.perf_counter()
        self.last = self.start
        self.steps: list[tuple[str, float]] = []

    def mark(self, step: str) -> None:
        now = time.perf_counter()
        self.steps.append((step, (now - self.last) * 1000))
        self.last = now

    def total_ms(self) -> float:
        return (time.perf_counter() - self.start) * 1000

    def log_if_slow(self, **fields) -> None:
        total = self.total_ms()
        if total < ORDER_SLOW_LOG_MS:
            return
        field_text = " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)
        step_text = " ".join(f"{step}={elapsed:.1f}ms" for step, elapsed in self.steps)
        print(f"[PERF] {self.name} total={total:.1f}ms {field_text} {step_text}")


async def get_product_remaining(product_id: int) -> int | None:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{PRODUCT_SERVICE_URL}/api/v1/products/{product_id}",
                timeout=5.0,
            )
        if resp.status_code == 200:
            return resp.json().get("remaining")
        return None
    except Exception:
        return None


async def send_notify_event(event_type: str, order) -> None:
    payload = {
        "event_type": event_type,
        "order_id": order.id,
        "order_number": order.order_number,
        "buyer_id": order.buyer_id,
        "store_id": order.store_id,
        "store_name": order.store_name,
        "product_names": [item.product_name for item in order.items],
        "pickup_expected_at": order.pickup_expected_at,
    }
    await publish_order_event(payload)


async def adjust_product_remaining(product_id: int, delta: int) -> tuple[bool, str]:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.patch(
                f"{PRODUCT_SERVICE_URL}/api/v1/products/{product_id}/remaining",
                params={"delta": delta},
                timeout=5.0
            )
        if resp.status_code == 409:
            detail = resp.json().get("detail", "재고가 부족합니다.")
            return False, detail
        resp.raise_for_status()
        return True, ""
    except Exception as e:
        print(f"[WARNING] 재고 수량 조정 실패 (product_id={product_id}, delta={delta}): {e}")
        return False, "재고 서비스 연결에 실패했습니다."


def generate_order_number(order_id: int, created_at: datetime.datetime) -> str:
    date_str = created_at.strftime("%Y%m%d")
    return f"PK-{date_str}-{order_id:04d}"


async def _publish_store_event(store_id: int | None, data: dict) -> None:
    if not store_id:
        return
    try:
        r = await get_redis()
        await r.publish(f"sse:store:{store_id}", json.dumps(data, default=str))
    except Exception as e:
        print(f"[SSE] store publish 실패 (store_id={store_id}): {e}")


@router.post("/", response_model=schemas.OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    order_data: schemas.OrderCreate,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    timer = StepTimer("create_order")
    created_order = None
    created_items: list[models.OrderItem] = []
    redis_reserved: list[tuple[int, int]] = []
    try:
        for item_data in order_data.items:
            if not item_data.product_id:
                continue
            result = await reserve_stock(item_data.product_id, item_data.quantity)
            if result is False:
                for pid, qty in redis_reserved:
                    await restore_stock(pid, qty)
                raise HTTPException(status_code=409, detail="재고가 부족합니다.")
            if result is True:
                redis_reserved.append((item_data.product_id, item_data.quantity))
            if result is None:
                remaining = await get_product_remaining(item_data.product_id)
                if remaining is None:
                    raise HTTPException(status_code=503, detail="재고 정보를 불러올 수 없습니다. 잠시 후 다시 시도해주세요.")
                if remaining < item_data.quantity:
                    raise HTTPException(status_code=409, detail=f"재고가 부족합니다. 현재 남은 수량: {remaining}개")
        timer.mark("stock_reserve")

        new_order = models.Order(
            # order_number is unique. A shared placeholder such as "TEMP" makes
            # concurrent inserts wait on the same unique index entry during flush.
            order_number=f"PENDING-{uuid.uuid4().hex}",
            buyer_id=current_user.user_id,
            store_id=order_data.store_id,
            store_name=order_data.store_name,
            status="pending",
            payment_method=order_data.payment_method,
            total_price=order_data.total_price,
            pickup_expected_at=order_data.pickup_expected_at,
        )
        db.add(new_order)
        await db.flush()
        timer.mark("order_flush")

        new_order.order_number = generate_order_number(new_order.id, new_order.created_at)

        for item_data in order_data.items:
            item = models.OrderItem(
                order_id=new_order.id,
                product_id=item_data.product_id,
                product_name=item_data.product_name,
                quantity=item_data.quantity,
                unit_price=item_data.unit_price,
            )
            db.add(item)
            created_items.append(item)

        await db.commit()
        timer.mark("db_commit")

        created_order = schemas.OrderResponse(
            id=new_order.id,
            order_number=new_order.order_number,
            buyer_id=new_order.buyer_id,
            store_id=new_order.store_id,
            store_name=new_order.store_name,
            status=new_order.status,
            payment_method=new_order.payment_method,
            total_price=new_order.total_price,
            pickup_expected_at=new_order.pickup_expected_at,
            created_at=new_order.created_at,
            items=[
                schemas.OrderItemResponse(
                    id=item.id,
                    product_id=item.product_id,
                    product_name=item.product_name,
                    quantity=item.quantity,
                    unit_price=item.unit_price,
                )
                for item in created_items
            ],
        )
        timer.mark("response_build")

        await publish_stock_deduct_event({
            "event_type": "stock_deduct",
            "order_id": created_order.id,
            "items": [
                {"product_id": item.product_id, "quantity": item.quantity}
                for item in created_order.items
                if item.product_id
            ],
        })
        timer.mark("stock_deduct_publish")
        await send_notify_event("order_confirmed", created_order)
        timer.mark("notify_publish")

        order_payload = created_order.model_dump(mode="json")
        await _publish_store_event(
            created_order.store_id,
            {"event_type": "new_order", "order": order_payload},
        )
        timer.mark("store_publish")

        return created_order
    finally:
        timer.log_if_slow(
            order_id=getattr(created_order, "id", None),
            store_id=order_data.store_id,
            item_count=len(order_data.items),
            reserved=len(redis_reserved),
        )


@router.get("/", response_model=List[schemas.OrderResponse])
async def list_orders(
    store_id: Optional[int] = None,
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    query = select(models.Order).options(selectinload(models.Order.items))
    if store_id is not None:
        query = query.filter(models.Order.store_id == store_id)
    else:
        query = query.filter(models.Order.buyer_id == current_user.user_id)
    if status is not None:
        query = query.filter(models.Order.status == status)
    query = query.order_by(models.Order.created_at.desc())
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/stats")
async def get_order_stats(
    store_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    KST = datetime.timezone(datetime.timedelta(hours=9))
    today = datetime.datetime.now(KST).date()
    yesterday = today - datetime.timedelta(days=1)

    async def daily_stats(date: datetime.date):
        result = await db.execute(
            select(
                func.coalesce(func.sum(models.Order.total_price), 0),
                func.count(models.Order.id),
            ).filter(
                models.Order.store_id == store_id,
                models.Order.status == "completed",
                cast(models.Order.created_at, SQLDate) == date,
            )
        )
        row = result.first()
        return int(row[0]), row[1]

    today_revenue, today_count = await daily_stats(today)
    yesterday_revenue, yesterday_count = await daily_stats(yesterday)

    return {
        "today_revenue": today_revenue,
        "today_count": today_count,
        "yesterday_revenue": yesterday_revenue,
        "yesterday_count": yesterday_count,
    }


@router.get("/internal/pending", include_in_schema=False)
async def list_pending_orders_internal(db: AsyncSession = Depends(get_db)):
    """내부 서비스 전용 — 인증 없이 pending 주문 목록 반환."""
    result = await db.execute(
        select(models.Order)
        .options(selectinload(models.Order.items))
        .filter(models.Order.status == "pending")
    )
    return result.scalars().all()


@router.get("/stream")
async def stream_store_orders(
    request: Request,
    store_id: int,
    current_user: CurrentUser = Depends(get_current_user),
):
    async def generator():
        r = await get_redis()
        pubsub = r.pubsub()
        channel = f"sse:store:{store_id}"
        await pubsub.subscribe(channel)
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=30.0)
                if message:
                    yield f"data: {message['data']}\n\n"
                else:
                    yield ": ping\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/{order_id}", response_model=schemas.OrderResponse)
async def get_order(
    order_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    result = await db.execute(
        select(models.Order)
        .options(selectinload(models.Order.items))
        .filter(models.Order.id == order_id)
    )
    order = result.scalars().first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@router.patch("/{order_id}/status", response_model=schemas.OrderResponse)
async def update_order_status(
    order_id: int,
    status_update: schemas.OrderStatusUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    result = await db.execute(
        select(models.Order)
        .options(selectinload(models.Order.items))
        .filter(models.Order.id == order_id)
    )
    order = result.scalars().first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    order.status = status_update.status
    await db.commit()

    refreshed = await db.execute(
        select(models.Order)
        .options(selectinload(models.Order.items))
        .filter(models.Order.id == order_id)
    )
    order = refreshed.scalars().first()

    if status_update.status == "completed":
        await send_notify_event("pickup_completed", order)
        await _publish_store_event(order.store_id, {"event_type": "order_removed", "order_id": order_id})
    elif status_update.status == "cancelled":
        for item in order.items:
            if item.product_id:
                await restore_stock(item.product_id, item.quantity)
                await adjust_product_remaining(item.product_id, item.quantity)
        await send_notify_event("order_cancelled", order)
        await _publish_store_event(order.store_id, {"event_type": "order_removed", "order_id": order_id})

    return order


@router.delete("/{order_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_order(
    order_id: int,
    cancelled_by: str = Query(default="buyer", description="취소 주체: buyer | seller"),
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    result = await db.execute(
        select(models.Order)
        .options(selectinload(models.Order.items))
        .filter(models.Order.id == order_id)
    )
    order = result.scalars().first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    store_id = order.store_id
    items_snapshot = list(order.items)
    order.status = "cancelled"
    await db.commit()

    for item in items_snapshot:
        if item.product_id:
            await restore_stock(item.product_id, item.quantity)
            await adjust_product_remaining(item.product_id, item.quantity)

    event_type = "order_cancelled_by_buyer" if cancelled_by == "buyer" else "order_cancelled_by_seller"
    await send_notify_event(event_type, order)
    await _publish_store_event(store_id, {"event_type": "order_removed", "order_id": order_id})
    return None
