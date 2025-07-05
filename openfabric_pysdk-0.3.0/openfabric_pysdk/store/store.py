import os
from typing import Any, Dict

from openfabric_pysdk.logger import logger
from openfabric_pysdk.store.kvdb import KeyValueDB
from openfabric_pysdk.store.lru import LRU


#######################################################
#  Store
#######################################################
class Store:
    __path: str = None
    __kvdbs: LRU = None
    __autodump: bool = None

    # ------------------------------------------------------------------------
    def __init__(self, path: str = None, autodump: bool = False, cache_size: int = 10):
        self.__path = path
        self.__autodump = autodump
        self.__cache_size = cache_size
        if self.__cache_size > 0:
            self.__kvdbs = LRU(10, self.dump)

    # ------------------------------------------------------------------------
    def dump(self, kvdb: KeyValueDB):
        logger.debug(f"Openfabric - evicting {kvdb}")
        kvdb.dump()

    # ------------------------------------------------------------------------
    def flush(self, name: str):
        kvdb = self.__instance(name)
        logger.debug(f"Openfabric - flush {kvdb}")
        kvdb.dump()

    # ------------------------------------------------------------------------
    def get(self, name, key, default=None) -> Any:
        kvdb = self.__instance(name)
        value = kvdb.get(key)
        if value:
            return value
        else:
            return default

    # ------------------------------------------------------------------------
    def set(self, name: str, key: str, val: Any):
        kvdb = self.__instance(name)
        kvdb.set(key, val)

    # ------------------------------------------------------------------------
    def rem(self, name: str, key: str):
        kvdb = self.__instance(name)
        kvdb.rem(key)

    # ------------------------------------------------------------------------
    def drop(self, name: str):
        kvdb = self.__instance(name)
        if self.__cache_size > 0:
            self.__kvdbs.rem(name)
        kvdb.drop()

    # ------------------------------------------------------------------------
    def all(self, name: str) -> Dict[str, Any]:
        kvdb = self.__instance(name)
        return kvdb.all()

    # ------------------------------------------------------------------------
    def reload(self, name: str):
        kvdb = self.__instance(name)
        kvdb.reload()

    # ------------------------------------------------------------------------
    def get_timestamp_last_persisted(self, name: str):
        db_path = f"{self.__path}/{name}.json"
        if os.path.isfile(db_path):
            return os.path.getmtime(db_path)
        else:
            return 0

    # ------------------------------------------------------------------------
    def __instance(self, name: str) -> KeyValueDB:
        if self.__cache_size > 0:
            kvdb = self.__kvdbs.get(name, None)
            if kvdb is None:
                kvdb = KeyValueDB(f"{name}", path=self.__path, autodump=self.__autodump)
                self.__kvdbs.put(name, kvdb)
            return kvdb
        else:
            return KeyValueDB(f"{name}", path=self.__path, autodump=self.__autodump)
