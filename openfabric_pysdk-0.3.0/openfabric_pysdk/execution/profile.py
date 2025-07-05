from openfabric_pysdk.flask import *


#######################################################
#  Profile
#######################################################
class Profile:
    host: str = None
    port: int = None
    debug: bool = False

    # ------------------------------------------------------------------------
    def __int__(self, host=None, port=None, debug=None):
        self.host = host
        self.port = port
        self.debug = debug
