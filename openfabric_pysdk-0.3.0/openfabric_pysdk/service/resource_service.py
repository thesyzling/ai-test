import magic
import os
import threading
from typing import Any, Optional

from openfabric_pysdk.logger import logger


#######################################################
#  Resource service
#######################################################
class ResourceService:
    __path = f"{os.getcwd()}/datastore"
    __store_location = "resources"
    __lock: threading.RLock = threading.RLock()


    @staticmethod
    def lock(executionId: str):
        ResourceService.__lock.acquire()
        if executionId is not None:
            ResourceService.__store_location = f"executions/{executionId}"

    @staticmethod
    def unlock():
        ResourceService.__store_location = "resources"
        ResourceService.__lock.release()


    # ------------------------------------------------------------------------
    @staticmethod
    def read(reid: Any):
        parts = reid.split('/')
        location = ""
        if len(parts) > 1:
            location = parts[1]
        
        # Prevent access to other directories or files like state.json
        if location != "executions" and location != "resources":
            logger.error(f"Invalid location: {location}")
            return None, None
        
        # rotate part by 1 and join, make the first element be the last
        parts = parts[1:] + parts[:1]        
        relative_path = "/".join(parts)

        path = f"{ResourceService.__path}/{relative_path}"
        
        # TODO: Should we return a failure instead of an empty file
        if not os.path.exists(f"{path}"):
            logger.warning(f"Resource not found: {reid}")
            return None, None

        mime = "application/json" if parts[-1].endswith(".json") else magic.Magic(mime=True).from_file(f"{path}")

        with open(f"{path}", 'rb') as f:
            return f.read(), mime

    # ------------------------------------------------------------------------
    @staticmethod
    def write(data: Any, resource_hash: str, resource_type: str, resource_encoding: str) -> Optional[str]:
        ResourceService.__lock.acquire()
        try:
            if data is None:
                # Nothing to serialize for this entry.
                return None

            if not os.path.exists(f"{ResourceService.__path}/{ResourceService.__store_location}"):
                os.makedirs(f"{ResourceService.__path}/{ResourceService.__store_location}")

            # Ensure data is in bytes format
            if isinstance(data, str):
                data = data.encode('utf-8')

            with open(f"{ResourceService.__path}/{ResourceService.__store_location}/{resource_type}_{resource_encoding}_{resource_hash}", 'wb') as f:
                f.write(data)
            return f"{resource_type}_{resource_encoding}_{resource_hash}/{ResourceService.__store_location}"
        finally:
            ResourceService.__lock.release()
