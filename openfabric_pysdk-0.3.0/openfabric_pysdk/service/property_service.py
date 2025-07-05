from typing import Any, Dict, List, Union

from openfabric_pysdk.loader.config import properties
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service.environment_service import EnvironmentService
from openfabric_pysdk.service.remote_service import RemoteService
from openfabric_pysdk.transport.schema.property_schema import Property, PropertySchema


#######################################################
#  Property service
#######################################################
class PropertyService:

    # ------------------------------------------------------------------------
    @staticmethod
    def get(name: str, default=None) -> Union[str, None]:
        """Fetches the property value by name, returns default if not found."""
        options = PropertyService.get_all()
        return next((option.value for option in options if option.name == name), default)

    # ------------------------------------------------------------------------
    @staticmethod
    def get_all() -> List['Property']:
        """Retrieves all properties from remote service or falls back to default."""
        app_id = EnvironmentService.get("APP_CONNECTION")
        dos_url = EnvironmentService.get("DOS_CONNECTION")

        if not app_id or not dos_url:
            logger.error(f"Failed to load properties. Missing or invalid environment variables: "
                          f"DOS_CONNECTION='{dos_url}', APP_CONNECTION='{app_id}'. Falling back to local properties.")
            return PropertyService.get_default()

        try:
            mapper = lambda data: PropertySchema().load(data, many=True)
            return (RemoteService.get(f"{dos_url}/api/v1/appProperty?filter[appProperty]=appId=={app_id}", mapper)
                    or PropertyService.get_default())
        except ConnectionError as e:
            logger.info(f"Failed to load properties from DOS node,"
                         f" falling back to local: {e}")

        return PropertyService.get_default()

    # ------------------------------------------------------------------------
    @staticmethod
    def get_default() -> List[Property]:
        """Retrieves the default local properties."""
        kvdb: List[Dict[str, Any]] = properties.all()
        return [Property(name=prop['name'], value=prop['value']) for prop in kvdb] if kvdb else []

# if __name__ == '__main__':
#     os.environ["DOS_CONNECTION"] = "https://dos.openfabric.network/"
#     os.environ["APP_CONNECTION"] = ""
#     print(PropertyService.get("key1", "100"))
#     print(PropertyService.get("key3", 200))
#     # print(PropertyService.get_all())
#     print(PropertyService.get_default())
