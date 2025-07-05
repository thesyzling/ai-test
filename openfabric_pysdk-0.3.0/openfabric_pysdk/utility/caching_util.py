from collections import OrderedDict
import time

#######################################################
#  Cache util
#######################################################
class LRUCacheMap:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = OrderedDict()
        self.timestamps = {}

    def get(self, key: str):
        if key not in self.cache:
            return None
        # move last to show it was recently used
        value = self.cache.pop(key)
        self.cache[key] = value
        return value

    def get_update_timestamp(self, key: str):
        if key not in self.timestamps:
            return None
        return self.timestamps[key]

    def put(self, key: str, value: any):
        if key in self.cache:
            # move last to show it was recently used
            self.cache.pop(key)
        elif len(self.cache) >= self.capacity:
            # free one item
            oldest_key, _ = self.cache.popitem(last=False)
            del self.timestamps[oldest_key]

        self.cache[key] = value
        self.timestamps[key] = time.time()

    def __repr__(self):
        return f"{self.__class__.__name__}({list(self.cache.items())})"