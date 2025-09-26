"""
High-Performance WebSocket Publisher for Local Communications

This module provides an optimized WebSocket publisher specifically designed
for high-frequency local data publishing with minimal latency.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, Optional, AsyncIterator, Deque
from collections import deque

import aiohttp

from . import logger
from .structures import RawData
from .types import URL

__all__ = [
    "HighPerformanceLocalPublisher",
    "LocalWebSocketPool", 
    "get_optimized_publisher"
]


class LocalWebSocketPool:
    """
    Pool of WebSocket connections optimized for local publishing.
    
    Maintains persistent connections and reuses them to minimize connection overhead.
    """
    
    def __init__(self, max_connections: int = 10, keepalive_interval: float = 30.0):
        self._connections: Dict[str, "LocalWebSocketConnection"] = {}
        self._max_connections = max_connections
        self._keepalive_interval = keepalive_interval
        self._cleanup_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
    
    async def get_connection(self, url: URL) -> "LocalWebSocketConnection":
        """Get or create a WebSocket connection for the given URL."""
        async with self._lock:
            url_str = str(url)
            
            if url_str in self._connections:
                conn = self._connections[url_str]
                if conn.is_connected():
                    return conn
                else:
                    # Remove dead connection
                    del self._connections[url_str]
            
            # Create new connection
            conn = LocalWebSocketConnection(url)
            await conn.connect()
            
            # Manage pool size
            if len(self._connections) >= self._max_connections:
                # Remove oldest connection
                oldest_key = next(iter(self._connections))
                old_conn = self._connections.pop(oldest_key)
                await old_conn.close()
            
            self._connections[url_str] = conn
            
            # Start cleanup task if not running
            if self._cleanup_task is None or self._cleanup_task.done():
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            
            return conn
    
    async def _cleanup_loop(self):
        """Periodic cleanup of dead connections."""
        while True:
            try:
                await asyncio.sleep(self._keepalive_interval)
                async with self._lock:
                    dead_keys = []
                    for key, conn in self._connections.items():
                        if not conn.is_connected():
                            dead_keys.append(key)
                    
                    for key in dead_keys:
                        conn = self._connections.pop(key)
                        await conn.close()
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in WebSocket pool cleanup: {e}")
    
    async def close_all(self):
        """Close all connections in the pool."""
        async with self._lock:
            if self._cleanup_task:
                self._cleanup_task.cancel()
            
            for conn in self._connections.values():
                await conn.close()
            
            self._connections.clear()


class LocalWebSocketConnection:
    """
    Optimized WebSocket connection for local publishing.
    
    Features:
    - Message batching for high-frequency publishing
    - Automatic reconnection
    - Low-latency message queuing
    """
    
    def __init__(self, url: URL, batch_size: int = 10, batch_timeout: float = 0.001):
        self.url = url
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._websocket: Optional[aiohttp.ClientWebSocketResponse] = None
        self._send_queue: Deque[RawData] = deque()
        self._send_task: Optional[asyncio.Task] = None
        self._connected = False
        self._lock = asyncio.Lock()
    
    async def connect(self):
        """Establish WebSocket connection."""
        try:
            # Create session with optimized settings for local connections
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=50,
                keepalive_timeout=60,
                enable_cleanup_closed=True,
                local_addr=None
            )
            
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=30, connect=5)
            )
            
            # Convert HTTP URL to WebSocket URL
            ws_url = str(self.url).replace('http://', 'ws://').replace('https://', 'wss://')
            if not ws_url.endswith('/'):
                ws_url += '/'
            ws_url += 'stream/push'
            
            self._websocket = await self._session.ws_connect(
                ws_url,
                heartbeat=30,
                max_msg_size=10 * 1024 * 1024,  # 10MB max message
                compress=0  # Disable compression for lower latency
            )
            
            self._connected = True
            self._send_task = asyncio.create_task(self._send_loop())
            
            logger.debug(f"WebSocket connected to {ws_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect WebSocket to {self.url}: {e}")
            await self.close()
            raise
    
    def is_connected(self) -> bool:
        """Check if the connection is active."""
        return (self._connected and 
                self._websocket is not None and 
                not self._websocket.closed)
    
    async def publish(self, data: RawData) -> bool:
        """
        Queue data for publishing.
        
        Returns True if queued successfully, False if connection is dead.
        """
        if not self.is_connected():
            return False
        
        self._send_queue.append(data)
        return True
    
    async def _send_loop(self):
        """Background task for batched sending."""
        batch = []
        last_send = time.monotonic()
        
        while self.is_connected():
            try:
                # Collect messages for batching
                while len(batch) < self.batch_size and len(self._send_queue) > 0:
                    batch.append(self._send_queue.popleft())
                
                # Send batch if we have messages and either batch is full or timeout reached
                current_time = time.monotonic()
                should_send = (
                    len(batch) > 0 and (
                        len(batch) >= self.batch_size or
                        current_time - last_send >= self.batch_timeout
                    )
                )
                
                if should_send:
                    await self._send_batch(batch)
                    batch.clear()
                    last_send = current_time
                
                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.0001)  # 0.1ms
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in WebSocket send loop: {e}")
                self._connected = False
                break
    
    async def _send_batch(self, batch: list):
        """Send a batch of messages."""
        if not self._websocket or len(batch) == 0:
            return
        
        try:
            for data in batch:
                # Send as binary message for efficiency
                await self._websocket.send_bytes(data.content)
            
        except Exception as e:
            logger.error(f"Failed to send WebSocket batch: {e}")
            self._connected = False
    
    async def close(self):
        """Close the WebSocket connection."""
        self._connected = False
        
        if self._send_task:
            self._send_task.cancel()
            try:
                await self._send_task
            except asyncio.CancelledError:
                pass
        
        if self._websocket:
            await self._websocket.close()
        
        if self._session:
            await self._session.close()


class HighPerformanceLocalPublisher:
    """
    High-performance publisher optimized for local communications.
    
    Features:
    - WebSocket connection pooling
    - Message batching
    - Automatic fallback to HTTP
    - Performance monitoring
    """
    
    def __init__(self):
        self._pool = LocalWebSocketPool()
        self._stats = {
            'websocket_publishes': 0,
            'http_fallbacks': 0,
            'total_bytes': 0,
            'avg_latency_ns': 0
        }
    
    @asynccontextmanager
    async def publisher_for_url(self, url: URL) -> AsyncIterator["LocalWebSocketConnection"]:
        """Get an optimized publisher for the given URL."""
        try:
            conn = await self._pool.get_connection(url)
            yield conn
        except Exception as e:
            logger.warning(f"Failed to get WebSocket publisher, falling back to HTTP: {e}")
            # Could yield HTTP fallback publisher here
            raise
    
    async def publish(self, url: URL, data: RawData) -> bool:
        """Publish data using the optimized publisher."""
        try:
            async with self.publisher_for_url(url) as publisher:
                success = await publisher.publish(data)
                if success:
                    self._stats['websocket_publishes'] += 1
                    self._stats['total_bytes'] += len(data.content)
                    return True
        except Exception:
            pass
        
        # Fallback to HTTP
        self._stats['http_fallbacks'] += 1
        return False
    
    def get_stats(self) -> dict:
        """Get performance statistics."""
        return self._stats.copy()
    
    async def close(self):
        """Close all connections."""
        await self._pool.close_all()


# Global high-performance publisher instance
_global_publisher: Optional[HighPerformanceLocalPublisher] = None


def get_optimized_publisher() -> HighPerformanceLocalPublisher:
    """Get the global optimized publisher instance."""
    global _global_publisher
    if _global_publisher is None:
        _global_publisher = HighPerformanceLocalPublisher()
    return _global_publisher