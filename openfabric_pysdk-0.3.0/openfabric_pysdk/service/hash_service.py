import hashlib
from typing import Any, Union


#######################################################
#  Hash service
#######################################################
class HashService:

    # ------------------------------------------------------------------------
    @staticmethod
    def fast_hash(obj: Any, hash_function: str = 'sha256'):
        hasher = hashlib.new(hash_function)
        flattened = HashService.__flatten_object(obj)
        hasher.update(str(flattened).encode('utf-8'))
        return hasher.hexdigest()

    # ------------------------------------------------------------------------
    @staticmethod
    def __flatten_object(obj: Any) -> Any:
        if isinstance(obj, dict):
            return tuple((k, HashService.__flatten_object(v)) for k, v in sorted(obj.items()))
        elif isinstance(obj, (list, tuple, set)):
            return tuple(HashService.__flatten_object(i) for i in obj)
        else:
            return obj

    @staticmethod
    def compute_hash(obj: Union[str, bytes, None]) -> Union[str, None]:
        if obj is None:
            return None

        if isinstance(obj, str):
            return hashlib.sha256(obj.encode()).hexdigest()

        return hashlib.sha256(obj).hexdigest()