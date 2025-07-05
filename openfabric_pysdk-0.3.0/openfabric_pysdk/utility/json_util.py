import hashlib
from typing import Union

from deepdiff import DeepDiff, Delta


#######################################################
#  Json util
#######################################################
class JsonUtil:

    # ------------------------------------------------------------------------
    @staticmethod
    def find_differences(source_json: Union[dict, list], target_json: Union[dict, list], deserializer=None):
        source = source_json if deserializer is None else deserializer(source_json)
        target = target_json if deserializer is None else deserializer(target_json)
        diff = DeepDiff(source, target,
                        verbose_level=2,
                        ignore_order=False,
                        ignore_type_in_groups=[(object,)])

        return {
            "old_hash": JsonUtil._get_map_hash(source_json, deserializer),
            "new_hash": JsonUtil._get_map_hash(target_json, deserializer),
            "delta": Delta(diff).to_dict()
        }

    # ------------------------------------------------------------------------
    @staticmethod
    def _get_map_hash(data: dict, deserializer=None):
        source = data if deserializer is None else deserializer(data)
        return hashlib.sha256(str(source).encode()).hexdigest()
