# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['openfabric_pysdk',
 'openfabric_pysdk.api',
 'openfabric_pysdk.app',
 'openfabric_pysdk.app.execution',
 'openfabric_pysdk.app.execution.ipc',
 'openfabric_pysdk.auth',
 'openfabric_pysdk.benchmark',
 'openfabric_pysdk.context',
 'openfabric_pysdk.engine',
 'openfabric_pysdk.execution',
 'openfabric_pysdk.fields',
 'openfabric_pysdk.flask',
 'openfabric_pysdk.flask.core',
 'openfabric_pysdk.flask.requests',
 'openfabric_pysdk.flask.rest',
 'openfabric_pysdk.flask.socket',
 'openfabric_pysdk.helper',
 'openfabric_pysdk.loader',
 'openfabric_pysdk.logger',
 'openfabric_pysdk.service',
 'openfabric_pysdk.store',
 'openfabric_pysdk.transport',
 'openfabric_pysdk.transport.rest',
 'openfabric_pysdk.transport.schema',
 'openfabric_pysdk.transport.socket',
 'openfabric_pysdk.transport.socket.handlers',
 'openfabric_pysdk.utility']

package_data = \
{'': ['*'], 'openfabric_pysdk': ['view/*']}

install_requires = \
['Flask-Cors>=3.0.10,<4.0.0',
 'Flask-RESTful>=0.3.9,<0.4.0',
 'Flask-SocketIO>=5.3.6,<6.0.0',
 'Flask>=2.0.1,<3.0.0',
 'Werkzeug>=2.0.3',
 'deepdiff>=7.0.1,<8.0.0',
 'flask-apispec>=0.11.0,<0.12.0',
 'gevent-websocket>=0.10.1,<0.11.0',
 'gevent>=22.10.2',
 'marshmallow-enum>=1.5.1,<2.0.0',
 'marshmallow-jsonapi>=0.24.0,<0.25.0',
 'marshmallow-jsonschema>=0.13.0,<0.14.0',
 'marshmallow<4.0.0',
 'pickleDB>=0.9.2,<0.10.0',
 'python-magic>=0.4.27,<0.5.0',
 'pyzmq>=25.1.1,<26.0.0',
 'runstats>=2.0.0,<3.0.0',
 'socketio-client>=0.7.2,<0.8.0',
 'tqdm>=4.62.3,<5.0.0',
 'web3>=7.5.0,<8.0.0']

setup_kwargs = {
    'name': 'openfabric-pysdk',
    'version': '0.3.0',
    'description': 'Openfabric Python SDK',
    'long_description': '==Openfabric-pysdk\n\n\nTrigger redeploy on node2:\n\ncurl --location \'https://github.node2.openfabric.network/webhook\' \\\n--header \'X-Hub-Signature: who cares\' \\\n--header \'Content-Type: application/json\' \\\n--data \'{\n    "ref": "master"\n}\'\n\n\n===Canceling a request\nWhen the user deletes a request that is not running the sdk handles it\'s removal\n\nWhen the user requests to delete the active request, a cancel notification may be sent to the app, to handle it\'s imediate exit.\nIf the application does not implement the cancel feature or does not handle it(returns False), then the sdk will initiate the process of a shameful exit.\nIf the application handles the cancel feature(returns True on cancel), then the sdk will give it a predefined time to finish before initiating the shameful exit(currently hardcoded at 1.5 seconds. will likely make it a property in the future)\n',
    'author': 'Andrei Tara',
    'author_email': 'andrei@openfabric.ai',
    'maintainer': 'None',
    'maintainer_email': 'None',
    'url': 'https://openfabric.ai',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.8,<4.0',
}


setup(**setup_kwargs)
