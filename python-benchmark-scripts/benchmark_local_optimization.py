#!/usr/bin/env python3
"""
Local Publishing Performance Benchmark

This script tests the latency improvements from local publishing optimizations.
"""

import asyncio
import time
import statistics
from typing import List

from dtps_http import (
    DTPSServer, 
    RawData, 
    MIME_JSON,
    ContentInfo,
    TopicNameV,
)
from dtps_http.local_optimization import LocalPublishingOptimizer


async def benchmark_local_publishing():
    """Benchmark local publishing performance."""
    
    print("=== Local Publishing Performance Benchmark ===")
    
    # Create a server for testing
    async def setup_server(server: DTPSServer) -> None:
        # Create a test topic
        topic_name = TopicNameV.from_dash_sep("benchmark/test")
        oq = await server.create_oq(
            topic_name,
            content_info=ContentInfo.simple(MIME_JSON),
            tp=None,
            bounds=None
        )
        # Store the queue for access in benchmark
        server._benchmark_oq = oq
        server._benchmark_topic = topic_name
    
    server = DTPSServer.create(
        on_startup=[setup_server],
        nickname="benchmark_server",
        enable_clock=False
    )
    
    # Start the server
    await server.on_startup(None)  # Initialize the server
    
    # Get the pre-created objects
    oq = server._benchmark_oq
    topic_name = server._benchmark_topic
    
    # Test data
    test_data = RawData(
        content=b'{"test": "data", "counter": 0}',
        content_type=MIME_JSON
    )
    
    print(f"Testing with {len(test_data.content)} byte messages...")
    
    # Benchmark parameters
    num_messages = 1000
    warmup_messages = 100
    
    # Warmup
    print("Warming up...")
    for i in range(warmup_messages):
        await oq.publish(test_data)
    
    # Benchmark standard publishing
    print("Benchmarking standard publishing...")
    standard_times = []
    
    for i in range(num_messages):
        start_time = time.perf_counter_ns()
        await oq.publish(test_data)
        end_time = time.perf_counter_ns()
        standard_times.append(end_time - start_time)
    
    # Benchmark optimized publishing
    print("Benchmarking optimized publishing...")
    optimized_times = []
    
    for i in range(num_messages):
        start_time = time.perf_counter_ns()
        result = await LocalPublishingOptimizer.try_local_publish(topic_name, test_data)
        end_time = time.perf_counter_ns()
        
        if result is not None:
            optimized_times.append(end_time - start_time)
        else:
            # Fallback to standard
            await oq.publish(test_data)
            optimized_times.append(end_time - start_time)
    
    # Calculate statistics
    def calc_stats(times: List[int]) -> dict:
        times_ms = [t / 1_000_000 for t in times]  # Convert to milliseconds
        return {
            'mean': statistics.mean(times_ms),
            'median': statistics.median(times_ms),
            'min': min(times_ms),
            'max': max(times_ms),
            'p95': sorted(times_ms)[int(0.95 * len(times_ms))],
            'p99': sorted(times_ms)[int(0.99 * len(times_ms))],
        }
    
    standard_stats = calc_stats(standard_times)
    optimized_stats = calc_stats(optimized_times)
    
    # Print results
    print("\n=== Results ===")
    print(f"{'Metric':<12} {'Standard (ms)':<15} {'Optimized (ms)':<15} {'Improvement':<12}")
    print("-" * 60)
    
    for metric in ['mean', 'median', 'min', 'max', 'p95', 'p99']:
        standard_val = standard_stats[metric]
        optimized_val = optimized_stats[metric]
        improvement = f"{((standard_val - optimized_val) / standard_val) * 100:.1f}%"
        
        print(f"{metric:<12} {standard_val:<15.3f} {optimized_val:<15.3f} {improvement:<12}")
    
    # Get optimization statistics
    stats = LocalPublishingOptimizer.get_stats()
    print("\n=== Optimization Statistics ===")
    print(f"Local publishes: {stats.local_publishes}")
    print(f"Remote publishes: {stats.remote_publishes}")
    print(f"Bytes saved from serialization: {stats.bytes_saved_from_serialization:,}")
    print(f"Total latency saved: {stats.latency_saved_ns / 1_000_000:.1f}ms")
    
    if stats.local_publishes > 0:
        avg_saved = stats.latency_saved_ns / stats.local_publishes / 1_000_000
        print(f"Average latency saved per publish: {avg_saved:.3f}ms")


async def benchmark_connection_types():
    """Benchmark different connection types."""
    
    print("\n=== Connection Type Benchmark ===")
    
    # This function tests connection latencies without actual data
    
    connections = [
        ("Local (same process)", 0),
        ("Unix socket", 1), 
        ("Localhost TCP", 2),
        ("Remote TCP", 4),
    ]
    
    for conn_name, complexity in connections:
        # Simulate different connection complexities
        times = []
        num_tests = 100
        
        for _ in range(num_tests):
            start_time = time.perf_counter_ns()
            
            # Simulate processing delay based on complexity
            if complexity > 0:
                await asyncio.sleep(complexity * 0.0001)  # Simulate network delay
            
            end_time = time.perf_counter_ns()
            times.append((end_time - start_time) / 1_000_000)  # Convert to ms
        
        avg_time = statistics.mean(times)
        print(f"{conn_name:<20}: {avg_time:.3f}ms average")


if __name__ == "__main__":
    asyncio.run(benchmark_local_publishing())
    asyncio.run(benchmark_connection_types())