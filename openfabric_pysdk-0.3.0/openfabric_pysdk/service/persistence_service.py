import os
import json
import shutil

from pathlib import Path
from typing import Any, Callable, Literal

from marshmallow import INCLUDE

from openfabric_pysdk.logger import logger

#######################################################
#  Property service
#######################################################
class PersistenceService:
    __path = f"{os.getcwd()}/datastore"
    # ------------------------------------------------------------------------
    @staticmethod
    def set_store_path(path: str):
        if not os.path.exists(path):
            os.makedirs(path)

        PersistenceService.__path = path

    @staticmethod
    def get_asset_timestamp(qid: str, key: Literal['in', 'out', 'ray']):
        asset_path = os.path.join(PersistenceService.__path, "executions", qid, str(key) + ".json")
        if not os.path.exists(asset_path):
            return None

        return os.path.getmtime(asset_path)
    
    @staticmethod
    def get_asset(qid: str, key: Literal['in', 'out', 'ray'], deserializer: Callable[[Any], Any] = None) -> Any:
        asset_path = os.path.join(PersistenceService.__path, "executions", qid, str(key) + ".json")

        if not os.path.exists(asset_path):
            return None

        try:
            with open(asset_path, 'r') as file:
                data = file.read()

                if data is None:
                    return None

                data = json.loads(data)

                return deserializer(data) if deserializer is not None else data
        except Exception as e:
            logger.error(f"Failed to read asset: {asset_path}. \n Reason: {e}")
            return None
    
    @staticmethod
    def set_asset(qid: str, key: Literal['in', 'out', 'ray'], data: str, serializer: Callable[[Any], Any] = None):
        asset_path = os.path.join(PersistenceService.__path, "executions", qid, str(key) + ".json")
        os.makedirs(os.path.dirname(asset_path), exist_ok=True)

        try:

            with open(asset_path, 'w') as file:
                if data is None:
                    return

                file.write(json.dumps(serializer(data) if serializer is not None else data))
        except Exception as e:
            logger.error(f"Failed to write asset: {asset_path}. \n Reason: {e}")
            return None
        
    @staticmethod
    def drop_assets(qid: str):
        asset_path = os.path.join(PersistenceService.__path, "executions", qid)
        if os.path.exists(asset_path):
            try:
                shutil.rmtree(asset_path)
            except Exception as e:
                logger.error(f"Failed to delete assets: {asset_path}. \n Reason: {e}")
