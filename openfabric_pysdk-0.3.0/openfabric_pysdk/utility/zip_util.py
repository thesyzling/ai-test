import json
import zlib
from typing import Any, Dict

from openfabric_pysdk.benchmark import measure_block_time


#######################################################
#  Zip util
#######################################################
class ZipUtil:

    # ------------------------------------------------------------------------
    @staticmethod
    def decompress(data: bytes) -> Dict[str, Any]:
        with measure_block_time("ZipUtil::decompress"):
            uncompressed = zlib.decompress(data)
            string: str = uncompressed.decode('utf-8')
            return json.loads(string)

    # ------------------------------------------------------------------------
    @staticmethod
    def compress(data: Dict[str, Any]) -> bytes:
        with measure_block_time("ZipUtil::compress"):
            string = json.dumps(data)
            return zlib.compress(string.encode('utf-8'))
