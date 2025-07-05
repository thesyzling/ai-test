from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.loader import getSchemaInst, getClass
from openfabric_pysdk.transport.schema import UserId
from openfabric_pysdk.utility import SchemaUtil


#######################################################
#  Config service
#######################################################
class ConfigService:

    # ------------------------------------------------------------------------
    @staticmethod
    def read(supervisor: Supervisor, uidc: UserId):
        state_config = supervisor.get_state_config()
        config = state_config.get(uidc.uid)

        if config is False:
            return getSchemaInst('config').dump(getClass('config')())
        return config

    # ------------------------------------------------------------------------
    @staticmethod
    def write(supervisor: Supervisor, uidc: UserId, config):
        if getSchemaInst('config').many is True and not isinstance(config, list):
            config = [config]
        else:
            config = config[0] if type(config) == list else config
        # Store
        state_config = supervisor.get_state_config()
        state_config.set(uidc.uid, getSchemaInst('config').dump(config))
        # Apply configuration
        ConfigService.apply(supervisor)

        return config

    # ------------------------------------------------------------------------
    @staticmethod
    def apply(supervisor: Supervisor):
        all_items = ConfigService.read_all(supervisor)
        supervisor.config_callback_function(all_items)

    # ------------------------------------------------------------------------
    @staticmethod
    def read_all(supervisor: Supervisor):
        # Reload form config file
        state_config = supervisor.get_state_config()
        items = state_config.all().items()
        return dict(map(lambda kv: (kv[0], getSchemaInst('config').load(kv[1])), items))
