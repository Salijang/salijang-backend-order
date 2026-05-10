# sallijang-backend-order

주문 처리 서비스입니다.

## 기술 스택

- **Python 3.11** / FastAPI
- **PostgreSQL** (asyncpg, SQLAlchemy, Alembic)
- **Redis** (재고 임시 예약)
- **AWS SQS** (재고 차감 이벤트 발행 / 차감 결과 수신)

## 주요 기능

- 주문 생성 (Saga 패턴: Redis 재고 예약 → 주문 DB 저장 → SQS 재고 차감 이벤트 발행)
- 주문 목록 / 상세 조회
- 주문 상태 변경 (완료 / 취소)
- 가게별 주문 통계 (일별)
- SSE 스트림으로 판매자에게 실시간 신규 주문 알림
- SQS 컨슈머: 재고 차감 실패 시 주문 자동 취소

## 주문 상태

```
pending → completed (픽업 완료)
       → cancelled  (구매자 또는 판매자 취소)
```

## API 엔드포인트

| Method | Path | 설명 |
|--------|------|------|
| POST | `/api/v1/orders/` | 주문 생성 |
| GET | `/api/v1/orders/` | 주문 목록 조회 |
| GET | `/api/v1/orders/{order_id}` | 주문 상세 조회 |
| PATCH | `/api/v1/orders/{order_id}` | 주문 상태 수정 |
| GET | `/api/v1/orders/stats` | 가게별 주문 통계 |
| GET | `/api/v1/orders/stream` | SSE 실시간 주문 스트림 (판매자용) |
| GET | `/api/v1/orders/internal/pending` | 픽업 예정 주문 조회 (내부 API) |

## 환경 변수

| 변수명 | 설명 |
|--------|------|
| `DB_HOST` | PostgreSQL 호스트 |
| `DB_PORT` | PostgreSQL 포트 (기본값: 5432) |
| `DB_USER` | DB 사용자명 |
| `DB_NAME` | DB 이름 |
| `DB_PASSWORD` | DB 비밀번호 (미설정 시 RDS IAM 인증) |
| `AWS_REGION` | AWS 리전 (기본값: ap-northeast-2) |
| `SQS_QUEUE_URL` | SQS 큐 URL |
| `REDIS_URL` | Redis 연결 URL |
| `PRODUCT_SERVICE_URL` | Product 서비스 URL |
| `NOTIFY_SERVICE_URL` | Notify 서비스 URL |
| `SECRET_KEY` | JWT 서명 키 |

## 로컬 실행

```bash
pip install -r requirements.txt
alembic upgrade head
uvicorn main:app --host 0.0.0.0 --port 8002 --reload
```

## Docker

```bash
docker build -t sallijang-order .
docker run -p 8002:8002 \
  -e DB_HOST=<host> \
  -e DB_USER=<user> \
  -e DB_PASSWORD=<password> \
  -e REDIS_URL=redis://redis:6379 \
  -e SQS_QUEUE_URL=<queue_url> \
  -e PRODUCT_SERVICE_URL=http://product-service:8001 \
  -e NOTIFY_SERVICE_URL=http://notify-service:8003 \
  -e SECRET_KEY=<secret> \
  sallijang-order
```
