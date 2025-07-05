from openfabric_pysdk.flask.core import *
from openfabric_pysdk.loader import *
from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.context import StateStatus
from openfabric_pysdk.logger import logger
from openfabric_pysdk.execution import Container, Profile

try:
    from importlib.metadata import version
    sdk_version=version('openfabric_pysdk')
except:
    sdk_version="unknown"

@webserver.route("/")
def index():
    if manifest.get("sdk") != sdk_version:
       manifest.set("sdk", sdk_version)
    return render_template("index.html", manifest=manifest.all())


class Starter:

    # ------------------------------------------------------------------------
    @staticmethod
    def ignite(debug: bool, host: str, port=5000):

        # Profile
        profile = Profile()
        profile.host = host
        profile.port = port
        profile.debug = debug

        # Start app execution
        supervisor = Supervisor()
        try:
            supervisor.set_status(StateStatus.STARTING)
            container = Container(profile, webserver)
            container.start(supervisor)
        except Exception as e:
            supervisor.set_status(StateStatus.CRASHED)
            logger.error(f"Openfabric - failed starting app: {e}")
