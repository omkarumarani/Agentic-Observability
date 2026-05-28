"""Pattern Library — asyncpg connection pool.

Vectors are handled as plain text strings in the format "[f1,f2,...]"
and cast to the pgvector `vector` type inside SQL using `$1::vector`.
This avoids requiring the pgvector Python package or numpy at runtime.
"""
import asyncpg
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://patterns:patterns123@pattern-db:5432/patterns",
)

_pool: Optional[asyncpg.Pool] = None


async def init_pool() -> None:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    logger.info("DB pool ready — %s", DATABASE_URL.split("@")[-1])


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("DB pool closed")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call init_pool() first")
    return _pool
