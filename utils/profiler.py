"""
Performance Profiling Utilities
-------------------------------
Decorators and tools for measuring and optimizing performance.
"""
import time
import functools
import logging
from typing import Callable, Any, Dict
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger('profiler')

# Global performance tracking
_performance_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
    'calls': 0,
    'total_time': 0.0,
    'min_time': float('inf'),
    'max_time': 0.0,
})

def profile(func: Callable) -> Callable:
    """
    Decorator to profile function execution time.
    
    Usage:
        @profile
        def my_function():
            pass
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Check if profiling is enabled
        try:
            from constants import PROFILING_ENABLED
            if not PROFILING_ENABLED:
                return func(*args, **kwargs)
        except ImportError:
            return func(*args, **kwargs)
        
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            elapsed = time.time() - start_time
            func_name = f"{func.__module__}.{func.__name__}"
            
            # Update stats
            stats = _performance_stats[func_name]
            stats['calls'] += 1
            stats['total_time'] += elapsed
            stats['min_time'] = min(stats['min_time'], elapsed)
            stats['max_time'] = max(stats['max_time'], elapsed)
            stats['avg_time'] = stats['total_time'] / stats['calls']
            
            # Log if execution took longer than 1 second
            if elapsed > 1.0:
                logger.debug(f"{func_name} took {elapsed:.2f}s")
    
    return wrapper

def get_performance_stats() -> Dict[str, Dict[str, Any]]:
    """Get current performance statistics."""
    return dict(_performance_stats)

def print_performance_report():
    """Print a formatted performance report."""
    try:
        from constants import PROFILING_OUTPUT_FILE
        output_file = PROFILING_OUTPUT_FILE
    except ImportError:
        output_file = "performance.log"
    
    if not _performance_stats:
        logger.info("No performance data collected")
        return
    
    # Sort by total time descending
    sorted_stats = sorted(
        _performance_stats.items(),
        key=lambda x: x[1]['total_time'],
        reverse=True
    )
    
    report_lines = [
        "\n" + "="*80,
        "PERFORMANCE REPORT",
        "="*80,
        f"{'Function':<50} {'Calls':>8} {'Total(s)':>10} {'Avg(ms)':>10} {'Min(ms)':>10} {'Max(ms)':>10}",
        "-"*80
    ]
    
    for func_name, stats in sorted_stats:
        report_lines.append(
            f"{func_name:<50} {stats['calls']:>8} "
            f"{stats['total_time']:>10.2f} "
            f"{stats['avg_time']*1000:>10.2f} "
            f"{stats['min_time']*1000:>10.2f} "
            f"{stats['max_time']*1000:>10.2f}"
        )
    
    report_lines.append("="*80)
    
    report = "\n".join(report_lines)
    
    # Print to console
    print(report)
    
    # Write to file
    try:
        with open(output_file, 'w') as f:
            f.write(report)
        logger.info(f"Performance report written to {output_file}")
    except Exception as e:
        logger.error(f"Could not write performance report: {e}")

def reset_performance_stats():
    """Reset all performance statistics."""
    _performance_stats.clear()

# Hot path tracking
_hot_paths: Dict[str, int] = defaultdict(int)

def track_hot_path(path_name: str):
    """Track frequently called code paths."""
    try:
        from constants import PROFILING_TRACK_HOT_PATHS
        if not PROFILING_TRACK_HOT_PATHS:
            return
    except ImportError:
        return
    
    _hot_paths[path_name] += 1

def get_hot_paths() -> Dict[str, int]:
    """Get hot path statistics."""
    return dict(sorted(_hot_paths.items(), key=lambda x: x[1], reverse=True))
