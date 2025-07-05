import asyncio
import functools
import gc
import timeit
from typing import Dict, List

from runstats import Statistics

from openfabric_pysdk.logger import logger

class TimerManager:
    # ------------------------------------------------------------------------
    def __init__(self):
        self._stats: Dict[str, Statistics] = {}
        self._lasts: Dict[str, float] = {}

    # ------------------------------------------------------------------------
    def add_timing(self, name: str, elapsed: float, no_print=True) -> Statistics:
        stats = self._stats.get(name, None)
        if stats is None:
            stats = Statistics()
            self._stats[name] = stats
        stats.push(elapsed)
        self._lasts[name] = elapsed

        if not no_print:
            logger.debug(f'Openfabric - timing "{name}": {elapsed}s')
        return stats

    # ------------------------------------------------------------------------
    def get_last(self, name: str) -> float:
        return self._lasts.get(name)

    # ------------------------------------------------------------------------
    def get_timing(self, name: str) -> Statistics:
        return self._stats.get(name)

    # ------------------------------------------------------------------------
    def get_all_timings(self) -> Dict[str, Statistics]:
        return self._stats

    # ------------------------------------------------------------------------
    def get_all_timings_json(self) -> List[Dict[str, str]]:
        res = []
        for name, stats in self._stats.items():
            count = len(stats)
            res.append({
                "name": name,
                'avg': f'{stats.mean():.2f}',
                'count': f'{count}',
                'stddev': f'{stats.stddev() if count > 1 else float("NaN"):.2f}',
                'min': f'{stats.minimum():.2f}',
                'max': f'{stats.maximum():.2f}',
            })
        return res

    # ------------------------------------------------------------------------
    def print_all_timing(self) -> None:
        for name in self._stats:
            self.print_timing(name)

    # ------------------------------------------------------------------------
    def print_timing(self, name: str) -> None:
        stats = self._stats.get(name, None)
        if stats is None:
            logger.debug(f'Openfabric - timing_name="{name}", avg=never_recorded')
        else:
            count = len(stats)
            logger.debug(f'Openfabric - timing_name="{name}", '
                          f'avg={stats.mean():.4g} '
                          f'count={count} '
                          f'stddev={stats.stddev() if count > 1 else float("NaN"):.4g} '
                          f'min={stats.minimum():.4g} '
                          f'max={stats.maximum():.4g} ')

    # ------------------------------------------------------------------------
    def clear_timings(self) -> None:
        self._stats.clear()


timer_manager = TimerManager()


# ------------------------------------------------------------------------
# Decorator to measure the time taken by bloc
class measure_block_time:
    def __init__(self, name: str, no_print=False, disable_gc=False):
        self.name = name
        self.no_print = no_print
        self.disable_gc = disable_gc

    def cur_elapsed(self) -> float:
        return timeit.default_timer() - self.start_time

    def __enter__(self):
        self.gcold = gc.isenabled()
        if self.disable_gc:
            gc.disable()
        self.start_time = timeit.default_timer()
        return self

    def __exit__(self, ty, val, tb):
        self.elapsed = self.cur_elapsed()
        if self.disable_gc and self.gcold:
            gc.enable()
        self.stats = timer_manager.add_timing(self.name, self.elapsed, no_print=self.no_print)
        return False  # re-raise any exceptions


# ------------------------------------------------------------------------
# Decorator to measure the time taken by a function
def measure_func_time(name=None, no_print=True, disable_gc=False):
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            gcold = gc.isenabled()  # Check if GC is enabled
            if disable_gc:
                gc.disable()  # Disable GC if requested

            start_time = timeit.default_timer()  # Start timing
            try:
                result = await func(*args, **kwargs)  # Await the async function
            finally:
                elapsed_time = timeit.default_timer() - start_time  # Calculate elapsed time
                if disable_gc and gcold:
                    gc.enable()  # Re-enable GC if it was disabled

                fname = name or func.__name__
                timer_manager.add_timing(fname, elapsed_time, no_print=no_print)  # Record the timing
            return result

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            gcold = gc.isenabled()  # Check if GC is enabled
            if disable_gc:
                gc.disable()  # Disable GC if requested

            start_time = timeit.default_timer()  # Start timing
            try:
                result = func(*args, **kwargs)  # Execute the sync function
            finally:
                elapsed_time = timeit.default_timer() - start_time  # Calculate elapsed time
                if disable_gc and gcold:
                    gc.enable()  # Re-enable GC if it was disabled

                fname = name or func.__name__
                timer_manager.add_timing(fname, elapsed_time, no_print=no_print)  # Record the timing
            return result

        # Choose the right wrapper based on whether the function is asynchronous
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
