from typing import Set

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.flask.core import request
from openfabric_pysdk.flask.socket import Namespace
from openfabric_pysdk.service.config_service import ConfigService
from openfabric_pysdk.transport.schema import UserIdSchema
from openfabric_pysdk.utility import ZipUtil


#################################################
#  ConfigureController
#################################################
class ConfigureController:
    __sessions: Set[str] = None
    __namespace: Namespace = None
    __supervisor: Supervisor = None

    def __init__(self, supervisor: Supervisor, namespace: Namespace, sessions: Set[str]):
        self.__supervisor = supervisor
        self.__namespace = namespace
        self.__sessions = sessions

    # --------------------------------------------------------------------------------
    def process(self, config: bytes):
        with measure_block_time("Socket::configure"):
            sid = request.sid

            dictionary = ZipUtil.decompress(config)
            config = dictionary.get('body', None)
            header = dictionary.get('header', dict())
            uid = header.get('uid', None)
            user = UserIdSchema().load(dict(uid=uid))

            if config is not None:
                ConfigService.write(self.__supervisor, user, config)
            else:
                # Return existing configuration
                # TODO: or should we have the option to clear it?
                config = ConfigService.read(self.__supervisor, user)

            self.__namespace.emit('settings', dict(uid=uid, config=config), room=sid)
