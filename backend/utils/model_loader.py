"""LRU-cached MLflow model loader for real-time inference."""
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

# Monotonically increasing counter — incrementing forces cache misses.
_cache_version = 0


@lru_cache(maxsize=5)
def _load_model(model_uri: str, version: int):
    """Internal cached loader. `version` parameter is only used as a cache key."""
    import mlflow.pyfunc
    logger.info(f"Loading MLflow model: {model_uri} (cache version {version})")
    return mlflow.pyfunc.load_model(model_uri)


def load_cached_model(model_uri: str):
    """Load an MLflow model, using LRU cache for repeated calls."""
    return _load_model(model_uri, _cache_version)


def clear_model_cache():
    """Invalidate the model cache (e.g. after a deployment update)."""
    global _cache_version
    _cache_version += 1
    _load_model.cache_clear()
    logger.info("Model cache cleared")
