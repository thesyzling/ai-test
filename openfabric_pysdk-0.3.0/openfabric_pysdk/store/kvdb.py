import os
from typing import Any, Dict, List, Union

import pickledb

from openfabric_pysdk.logger import logger

#######################################################
#  KeyValueDB
#######################################################
class KeyValueDB:
    __name: str = None
    __db_path: str = None
    __autodump: bool = None
    __db: pickledb.PickleDB = None
    __dirty: bool = False

    # ------------------------------------------------------------------------
    def __init__(self, name: str, path: str = None, autodump: bool = False):

        if path is None:
            path = f"{os.getcwd()}"

        self.__name = name
        self.__autodump = autodump
        self.__db_path = f"{path}/{name}.json"
        self.__dirty = False

        # extract full path and create it if not exists
        full_path = os.path.dirname(self.__db_path)
        if not os.path.exists(full_path):
            os.makedirs(full_path)

        try:
            self.__db = pickledb.load(self.__db_path, False, sig=False)
        except Exception as e:
            logger.error(f"Openfabric - store {self.__db_path} is corrupted, recreate it {e}")
            os.remove(self.__db_path)
            self.__db = pickledb.load(self.__db_path, False, sig=False)

    # ------------------------------------------------------------------------
    def reload(self):
        try:
            self.__db = pickledb.load(self.__db_path, False, sig=False)
        except BaseException as e:
            logger.error(
                f"Openfabric - store {self.__db_path} is corrupted, recreate it: {e}")
            os.remove(self.__db_path)
            self.__db = pickledb.load(self.__db_path, False, sig=False)
        self.__dirty = False

    # ------------------------------------------------------------------------
    def exists(self, key: str):
        return self.__db.exists(key)

    # ---------------------------`---------------------------------------------
    def rem(self, key: str):
        if not self.__dirty and self.__db.exists(key):
            self.__dirty = True
        self.__db.rem(key)
        if self.__autodump:
            self.dump()

    # ---------------------------`---------------------------------------------
    def drop(self):
        self.__db.deldb()
        if os.path.isfile(self.__db_path):
            os.remove(self.__db_path)

    # ------------------------------------------------------------------------
    def get(self, key: str):
        return self.__db.get(key)

    # ------------------------------------------------------------------------
    def keys(self):
        return self.__db.getall()

    # ------------------------------------------------------------------------
    def set(self, key: str, val: Any):
        if not self.__dirty and self.__db.get(key) != val:
            self.__dirty = True
        self.__db.set(key, val)
        if self.__autodump:
            self.dump()

    # ------------------------------------------------------------------------
    def dump(self):
        try:
            if self.__dirty:
                self.__db.dump()
                self.__dirty = False
        except:
            logger.error(f"Openfabric - store {self.__db_path}. Failed to persist!")

    # ------------------------------------------------------------------------
    def all(self) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        return self.__db.db
