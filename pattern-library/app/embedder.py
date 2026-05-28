"""Pattern Library — embedding generation via Ollama nomic-embed-text.

Dimensions: 768 (nomic-embed-text:latest).
Degrades gracefully when Ollama is offline — returns None.
"""
import httpx
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

LOCAL_LLM_URL = os.getenv("LOCAL_LLM_URL", "http://local-llm:11434")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")


def vec_to_str(vec: list[float]) -> str:
    """Format a Python float list as a PostgreSQL vector literal: [f1,f2,...]"""
    return "[" + ",".join(f"{v:.8f}" for v in vec) + "]"


async def embed_text(text: str) -> Optional[list[float]]:
    """
    Generate a 768-dim embedding for the given text using Ollama.
    Returns None when Ollama is unavailable — callers store NULL embeddings
    and regenerate later via POST /admin/embed-all.
    """
    if not text or not text.strip():
        return None
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{LOCAL_LLM_URL}/api/embeddings",
                json={"model": EMBED_MODEL, "prompt": text.strip()},
            )
            resp.raise_for_status()
            data = resp.json()
            embedding = data.get("embedding")
            if embedding:
                return [float(v) for v in embedding]
            logger.warning("Empty embedding returned by Ollama for model=%s", EMBED_MODEL)
    except Exception as exc:
        logger.warning("Embedding generation failed (Ollama may be offline): %s", exc)
    return None
