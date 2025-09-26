"""
Enhanced Client Publishing with Local Optimization

This module enhances the DTPSClient publish methods to use local optimization
when publishing to queues in the same process.
"""

import time
from typing import Optional
from urllib.parse import urlparse

from . import logger
from .client import DTPSClient
from .local_optimization import LocalPublishingOptimizer, should_use_local_optimization
from .structures import RawData, DataSaved
from .types import URL

__all__ = ["enhance_client_with_local_optimization"]


# Store original methods for fallback
_original_publish = None
_original_publish_json = None 
_original_publish_cbor = None


async def enhanced_publish(self: DTPSClient, url: URL, data: RawData) -> Optional[DataSaved]:
    """
    Enhanced publish method that attempts local optimization first.
    
    Falls back to original HTTP-based publishing if local optimization fails.
    """
    
    # Try to extract topic name from URL for local optimization
    topic_name = None
    try:
        # Parse URL to detect if this might be a local operation
        parsed = urlparse(str(url))
        
        # Check if this is a local connection (unix socket or localhost)
        is_local = (
            parsed.scheme == 'http+unix' or
            (parsed.hostname in ['localhost', '127.0.0.1', '::1']) or
            parsed.hostname is None
        )
        
        if is_local and should_use_local_optimization(complexity=0):
            # Try to extract topic name from path
            # URL format is typically: http://host/topic/path
            path_parts = parsed.path.strip('/').split('/')
            if path_parts:
                from .structures import TopicNameV
                try:
                    topic_name = TopicNameV.from_dash_sep('/'.join(path_parts))
                except Exception:
                    # If we can't parse the topic name, fall back to HTTP
                    pass
        
    except Exception as e:
        logger.debug(f"Could not parse URL for local optimization: {e}")
    
    # Attempt local optimization if we have a topic name
    if topic_name:
        start_time = time.time_ns()
        local_result = await LocalPublishingOptimizer.try_local_publish(
            topic_name, data, clocks=None
        )
        
        if local_result is not None:
            end_time = time.time_ns()
            logger.debug(f"Local publish succeeded in {(end_time - start_time)/1_000_000:.2f}ms")
            return local_result
    
    # Fall back to original HTTP-based publishing
    logger.debug(f"Using HTTP fallback for publish to {url}")
    LocalPublishingOptimizer._local_registry.record_remote_publish()
    return await _original_publish(self, url, data)


async def enhanced_publish_json(self: DTPSClient, url: URL, data: dict) -> Optional[DataSaved]:
    """Enhanced publish_json method with local optimization."""
    
    # Convert to RawData first for potential local optimization
    from .structures import RawData
    from .constants import MIME_JSON
    import json
    
    json_data = json.dumps(data).encode('utf-8')
    rd = RawData(content=json_data, content_type=MIME_JSON)
    
    return await enhanced_publish(self, url, rd)


async def enhanced_publish_cbor(self: DTPSClient, url: URL, data) -> Optional[DataSaved]:
    """Enhanced publish_cbor method with local optimization."""
    
    # Convert to RawData first for potential local optimization
    from .structures import RawData
    from .constants import MIME_CBOR
    try:
        import cbor2
        cbor_data = cbor2.dumps(data)
    except ImportError:
        # Fallback to original method if cbor2 not available
        return await _original_publish_cbor(self, url, data)
    rd = RawData(content=cbor_data, content_type=MIME_CBOR)
    
    return await enhanced_publish(self, url, rd)


def enhance_client_with_local_optimization():
    """
    Enhance DTPSClient with local publishing optimization.
    
    This function monkey-patches the DTPSClient methods to add local optimization
    while maintaining backward compatibility.
    """
    global _original_publish, _original_publish_json, _original_publish_cbor
    
    # Store original methods
    _original_publish = DTPSClient.publish
    _original_publish_json = getattr(DTPSClient, 'publish_json', None)
    _original_publish_cbor = getattr(DTPSClient, 'publish_cbor', None)
    
    # Replace with enhanced methods
    DTPSClient.publish = enhanced_publish
    
    if _original_publish_json:
        DTPSClient.publish_json = enhanced_publish_json
    
    if _original_publish_cbor:
        DTPSClient.publish_cbor = enhanced_publish_cbor
    
    logger.info("Enhanced DTPSClient with local publishing optimization")


# Auto-apply enhancement when module is imported
enhance_client_with_local_optimization()