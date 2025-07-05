import uuid

from openfabric_pysdk.context import State
from openfabric_pysdk.context.ray import Ray
from openfabric_pysdk.loader import InputClass, OutputClass, getClass


class AppModel:
    ray: Ray
    state: State
    request: InputClass
    response: OutputClass

    def __init__(self, *args, **kwargs):
        qid = kwargs.get('qid', uuid.uuid4().hex)
        self.ray = kwargs.get('ray', Ray(qid=qid))
        self.state = kwargs.get('state', State())
        self.request = kwargs.get('input', getClass("in")())
        self.response = kwargs.get('output', getClass("out")())
