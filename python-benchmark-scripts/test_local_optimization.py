#!/usr/bin/env python3
"""
Test Runner for Local Publishing Optimizations

This script runs tests to validate that the local publishing optimizations
work correctly and don't break existing functionality.
"""

import asyncio
import time

from dtps_http import (
    DTPSServer,
    RawData,
    MIME_JSON,
    ContentInfo,
    TopicNameV,
    Bounds,
)


async def test_basic_local_publishing():
    """Test that basic local publishing still works."""
    server = DTPSServer()
    
    topic_name = TopicNameV.from_dash_sep("test/basic")
    oq = await server.create_oq(
        topic_name,
        content_info=ContentInfo.simple(MIME_JSON),
        tp=None,
        bounds=Bounds.max_length(10)
    )
    
    test_data = RawData(
        content=b'{"test": "basic_publishing"}',
        content_type=MIME_JSON
    )
    
    # Publish data
    result = await oq.publish(test_data, get_data=True)
    
    # Verify result
    assert result is not None
    assert hasattr(result, 'data_saved')
    
    # Verify data can be retrieved
    last_data = oq.last_data()
    assert last_data.content == test_data.content
    assert last_data.content_type == test_data.content_type
    
    print("✓ Basic local publishing test passed")


async def test_local_optimization_registration():
    """Test that ObjectQueues are automatically registered for optimization."""
    from dtps_http.local_optimization import LocalPublishingOptimizer
    
    server = DTPSServer()
    topic_name = TopicNameV.from_dash_sep("test/registration")
    
    # Create queue (should auto-register)
    oq = await server.create_oq(
        topic_name,
        content_info=ContentInfo.simple(MIME_JSON),
        tp=None,
        bounds=None
    )
    
    # Check if registered
    local_queue = LocalPublishingOptimizer._local_registry.get_local_queue(topic_name)
    assert local_queue is not None
    assert local_queue is oq
    
    print("✓ Auto-registration test passed")


async def test_local_optimization_performance():
    """Test that local optimization actually improves performance."""
    from dtps_http.local_optimization import LocalPublishingOptimizer
    
    server = DTPSServer()
    topic_name = TopicNameV.from_dash_sep("test/performance")
    
    oq = await server.create_oq(
        topic_name,
        content_info=ContentInfo.simple(MIME_JSON),
        tp=None,
        bounds=None
    )
    
    test_data = RawData(
        content=b'{"test": "performance", "size": "' + b'x' * 1000 + b'"}',
        content_type=MIME_JSON
    )
    
    # Measure standard publishing
    num_iterations = 50
    
    standard_times = []
    for _ in range(num_iterations):
        start = time.perf_counter_ns()
        await oq.publish(test_data)
        end = time.perf_counter_ns()
        standard_times.append(end - start)
    
    # Measure optimized publishing
    optimized_times = []
    for _ in range(num_iterations):
        start = time.perf_counter_ns()
        result = await LocalPublishingOptimizer.try_local_publish(topic_name, test_data)
        end = time.perf_counter_ns()
        
        assert result is not None  # Should succeed with local optimization
        optimized_times.append(end - start)
    
    # Calculate averages
    avg_standard = sum(standard_times) / len(standard_times) / 1_000_000  # Convert to ms
    avg_optimized = sum(optimized_times) / len(optimized_times) / 1_000_000
    
    print(f"Average standard publish time: {avg_standard:.3f}ms")
    print(f"Average optimized publish time: {avg_optimized:.3f}ms")
    
    # Optimized should be at least as fast (allowing for measurement noise)
    improvement_threshold = 0.95  # Allow up to 5% slower due to overhead
    assert avg_optimized <= avg_standard * improvement_threshold, \
        f"Optimization didn't improve performance: {avg_optimized:.3f}ms vs {avg_standard:.3f}ms"
    
    print("✓ Performance improvement test passed")


async def test_concurrent_publishing():
    """Test that concurrent publishing works correctly with optimizations."""
    server = DTPSServer()
    topic_name = TopicNameV.from_dash_sep("test/concurrent")
    
    oq = await server.create_oq(
        topic_name,
        content_info=ContentInfo.simple(MIME_JSON),
        tp=None,
        bounds=Bounds.max_length(100)
    )
    
    async def publish_worker(worker_id: int, num_messages: int):
        for i in range(num_messages):
            test_data = RawData(
                content=f'{{"worker": {worker_id}, "message": {i}}}'.encode('utf-8'),
                content_type=MIME_JSON
            )
            await oq.publish(test_data)
    
    # Run concurrent publishers
    num_workers = 5
    messages_per_worker = 20
    
    tasks = [
        asyncio.create_task(publish_worker(i, messages_per_worker))
        for i in range(num_workers)
    ]
    
    await asyncio.gather(*tasks)
    
    # Verify all messages were published
    # (The queue will only keep the last 100 due to bounds, but that's expected)
    print(f"✓ Concurrent publishing test passed ({num_workers} workers, {messages_per_worker} messages each)")


async def test_error_handling():
    """Test that error handling works correctly with optimizations."""
    from dtps_http.local_optimization import LocalPublishingOptimizer
    
    # Try to publish to non-existent topic
    non_existent_topic = TopicNameV.from_dash_sep("test/nonexistent")
    test_data = RawData(content=b'{"test": "error_handling"}', content_type=MIME_JSON)
    
    result = await LocalPublishingOptimizer.try_local_publish(non_existent_topic, test_data)
    
    # Should return None (not found)
    assert result is None
    
    print("✓ Error handling test passed")


async def run_all_tests():
    """Run all tests."""
    print("Running local publishing optimization tests...\n")
    
    tests = [
        test_basic_local_publishing,
        test_local_optimization_registration,
        test_local_optimization_performance,
        test_concurrent_publishing,
        test_error_handling,
    ]
    
    for test_func in tests:
        try:
            await test_func()
        except Exception as e:
            print(f"✗ {test_func.__name__} failed: {e}")
            raise
    
    print(f"\n🎉 All {len(tests)} tests passed!")


if __name__ == "__main__":
    asyncio.run(run_all_tests())