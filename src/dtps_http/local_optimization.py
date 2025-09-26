"""
Local Publishing Optimization

This module provides optimizations for publishing data locally within the same process,
bypassing HTTP overhead when publisher and subscriber are in the same address space.
"""

import threading
import time
import weakref
from dataclasses import dataclass
from typing import Dict, Optional

from . import logger
from .structures import RawData, DataSaved, TopicNameV, Clocks
from .object_queue import ObjectQueue

__all__ = [
    "LocalPublishingRegistry", 
    "LocalPublishingOptimizer",
    "should_use_local_optimization",
]


@dataclass
class LocalPublishingStats:
    """Statistics for local publishing optimization."""
    local_publishes: int = 0
    remote_publishes: int = 0
    bytes_saved_from_serialization: int = 0
    latency_saved_ns: int = 0


class LocalPublishingRegistry:
    """
    Registry to track object queues within the same process for direct memory publishing.
    
    This allows us to bypass HTTP serialization/deserialization and network overhead
    when publishing to queues in the same process.
    """
    
    def __init__(self):
        self._lock = threading.RLock()
        # Use WeakValueDictionary to automatically clean up dead references
        self._local_queues: Dict[str, weakref.ref] = {}
        self._process_id = threading.get_ident()
        self._stats = LocalPublishingStats()
        
    def register_queue(self, topic_name: TopicNameV, queue: ObjectQueue) -> None:
        """Register an ObjectQueue for local optimization."""
        with self._lock:
            key = topic_name.as_relative_url()
            # Use weak reference to avoid circular references
            self._local_queues[key] = weakref.ref(queue, self._cleanup_callback(key))
            logger.debug(f"Registered local queue for topic: {key}")
    
    def unregister_queue(self, topic_name: TopicNameV) -> None:
        """Unregister an ObjectQueue."""
        with self._lock:
            key = topic_name.as_relative_url()
            if key in self._local_queues:
                del self._local_queues[key]
                logger.debug(f"Unregistered local queue for topic: {key}")
    
    def get_local_queue(self, topic_name: TopicNameV) -> Optional[ObjectQueue]:
        """Get a local ObjectQueue if available in this process."""
        with self._lock:
            key = topic_name.as_relative_url()
            if key in self._local_queues:
                queue_ref = self._local_queues[key]
                queue = queue_ref()
                if queue is not None:
                    return queue
                else:
                    # Clean up dead reference
                    del self._local_queues[key]
            return None
    
    def _cleanup_callback(self, key: str):
        """Callback for weak reference cleanup."""
        def cleanup(ref):
            with self._lock:
                if key in self._local_queues and self._local_queues[key] is ref:
                    del self._local_queues[key]
                    logger.debug(f"Auto-cleaned up local queue reference: {key}")
        return cleanup
    
    def get_stats(self) -> LocalPublishingStats:
        """Get statistics about local publishing optimization."""
        with self._lock:
            return LocalPublishingStats(
                local_publishes=self._stats.local_publishes,
                remote_publishes=self._stats.remote_publishes,
                bytes_saved_from_serialization=self._stats.bytes_saved_from_serialization,
                latency_saved_ns=self._stats.latency_saved_ns
            )
    
    def record_local_publish(self, data_size: int, latency_saved_ns: int) -> None:
        """Record statistics for a local publish operation."""
        with self._lock:
            self._stats.local_publishes += 1
            self._stats.bytes_saved_from_serialization += data_size * 2  # Avoid serialize + deserialize
            self._stats.latency_saved_ns += latency_saved_ns
    
    def record_remote_publish(self) -> None:
        """Record statistics for a remote publish operation."""
        with self._lock:
            self._stats.remote_publishes += 1


# Global registry instance
_local_registry = LocalPublishingRegistry()


class LocalPublishingOptimizer:
    """
    Optimizer that provides direct memory publishing for local object queues.
    
    This class intercepts publish operations and routes them directly to local
    ObjectQueues when possible, bypassing HTTP serialization overhead.
    """
    
    @staticmethod
    async def try_local_publish(
        topic_name: TopicNameV, 
        data: RawData, 
        clocks: Optional[Clocks] = None
    ) -> Optional[DataSaved]:
        """
        Attempt to publish data directly to a local ObjectQueue.
        
        Returns DataSaved if successful, None if local optimization not possible.
        """
        local_queue = _local_registry.get_local_queue(topic_name)
        if local_queue is None:
            return None
        
        start_time = time.time_ns()
        
        try:
            # Publish directly to the local queue, bypassing HTTP entirely
            result = await local_queue.publish(data, get_data=True)
            
            end_time = time.time_ns()
            latency_saved = max(1000000, end_time - start_time)  # Assume at least 1ms saved from HTTP
            
            # Record statistics
            _local_registry.record_local_publish(len(data.content), latency_saved)
            
            logger.debug(f"Local publish optimized for {topic_name.as_relative_url()}: "
                        f"saved {latency_saved/1_000_000:.2f}ms")
            
            return result.data_saved if result else None
            
        except Exception as e:
            logger.warning(f"Local publish optimization failed for {topic_name.as_relative_url()}: {e}")
            return None
    
    @staticmethod
    def register_queue(topic_name: TopicNameV, queue: ObjectQueue) -> None:
        """Register a queue for local optimization."""
        _local_registry.register_queue(topic_name, queue)
    
    @staticmethod
    def unregister_queue(topic_name: TopicNameV) -> None:
        """Unregister a queue from local optimization."""
        _local_registry.unregister_queue(topic_name)
    
    @staticmethod
    def get_stats() -> LocalPublishingStats:
        """Get optimization statistics."""
        return _local_registry.get_stats()


def should_use_local_optimization(connection_complexity: int = 0) -> bool:
    """
    Determine if local optimization should be used based on connection characteristics.
    
    Args:
        connection_complexity: 0 for local, 1+ for network hops
        
    Returns:
        True if local optimization should be attempted
    """
    # Only use local optimization for truly local connections
    return connection_complexity == 0


# Monkey patch ObjectQueue to auto-register for local optimization
_original_object_queue_init = ObjectQueue.__init__

def _enhanced_object_queue_init(self, *args, **kwargs):
    """Enhanced ObjectQueue.__init__ that auto-registers for local optimization."""
    _original_object_queue_init(self, *args, **kwargs)
    
    # Auto-register this queue for local optimization
    if hasattr(self, '_name'):
        LocalPublishingOptimizer.register_queue(self._name, self)
        logger.debug(f"Auto-registered ObjectQueue for local optimization: {self._name.as_relative_url()}")

# Apply the monkey patch
ObjectQueue.__init__ = _enhanced_object_queue_init


# Enhanced cleanup when ObjectQueue is destroyed
_original_object_queue_del = getattr(ObjectQueue, '__del__', None)

def _enhanced_object_queue_del(self):
    """Enhanced ObjectQueue.__del__ that auto-unregisters from local optimization."""
    if hasattr(self, '_name'):
        LocalPublishingOptimizer.unregister_queue(self._name)
    
    if _original_object_queue_del:
        _original_object_queue_del(self)

ObjectQueue.__del__ = _enhanced_object_queue_del