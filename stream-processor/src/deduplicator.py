import time
import logging
from typing import Optional
import redis

logger = logging.getLogger(__name__)

class Deduplicator:
    """
    Stateful deduplication engine using Redis with TTL expiration.
    Falls back gracefully to an in-memory TTL dictionary if Redis is temporarily unreachable.
    """
    def __init__(self, host: str, port: int, db: int = 0, ttl_seconds: int = 3600):
        self.ttl_seconds = ttl_seconds
        self.redis_client: Optional[redis.Redis] = None
        self._memory_cache = {}  # {key: expiration_timestamp}
        
        try:
            r = redis.Redis(host=host, port=port, db=db, socket_timeout=2, decode_responses=True)
            r.ping()
            self.redis_client = r
            logger.info(f"Connected to Redis state store for deduplication at {host}:{port}")
        except Exception as e:
            logger.warning(f"Could not connect to Redis ({e}). Using in-memory TTL deduplication.")
            self.redis_client = None

    def is_duplicate(self, event_id: str) -> bool:
        """
        Atomically checks if event_id has been seen within TTL window.
        Returns True if duplicate, False if new (and marks it seen with TTL).
        """
        if not event_id:
            return False

        if self.redis_client:
            try:
                # SET key 1 NX EX ttl_seconds
                # Returns True if key was set (i.e. was NOT already present)
                # Returns None / False if key already existed
                key = f"dedup:{event_id}"
                was_set = self.redis_client.set(key, "1", ex=self.ttl_seconds, nx=True)
                return not was_set
            except Exception as e:
                logger.warning(f"Redis error during dedup check: {e}. Falling back to memory.")

        # In-memory TTL fallback
        now = time.time()
        # Clean expired entries occasionally
        if len(self._memory_cache) > 20000:
            self._cleanup_memory_cache(now)
            
        exp = self._memory_cache.get(event_id)
        if exp and exp > now:
            return True
            
        self._memory_cache[event_id] = now + self.ttl_seconds
        return False

    def _cleanup_memory_cache(self, now: float):
        expired = [k for k, exp in self._memory_cache.items() if exp <= now]
        for k in expired:
            del self._memory_cache[k]
