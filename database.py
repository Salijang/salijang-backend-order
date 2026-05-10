import os
import boto3
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import event

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "adminuser")
DB_NAME = os.environ.get("DB_NAME", "pickupdb")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
DB_POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "3"))
DB_MAX_OVERFLOW = int(os.environ.get("DB_MAX_OVERFLOW", "2"))
DB_POOL_TIMEOUT = int(os.environ.get("DB_POOL_TIMEOUT", "3"))

_USE_IAM = not bool(DB_PASSWORD)


def _generate_iam_token() -> str:
    return boto3.client("rds", region_name=AWS_REGION).generate_db_auth_token(
        DBHostname=DB_HOST,
        Port=int(DB_PORT),
        DBUsername=DB_USER,
    )


if _USE_IAM:
    _url = f"postgresql+asyncpg://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    _connect_args = {"ssl": "require"}
else:
    _url = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    _connect_args = {}

engine = create_async_engine(
    _url,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT,
    pool_recycle=600,
    pool_pre_ping=True,
    connect_args=_connect_args,
    echo=False,
)

if _USE_IAM:
    @event.listens_for(engine.sync_engine, "do_connect")
    def provide_iam_token(dialect, conn_rec, cargs, cparams):
        cparams["password"] = _generate_iam_token()


SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
Base = declarative_base()


async def get_db():
    async with SessionLocal() as session:
        yield session
