import pickle

from openfabric_pysdk.app.execution.ipc.actions import DispatchActions


class ActionEncoder:

    # ------------------------------------------------------------------------
    @staticmethod
    def add(qid: str):
        return pickle.dumps({'action': DispatchActions.ADD, 'data': qid})

    # ------------------------------------------------------------------------
    @staticmethod
    def check_request(qid: str):
        return pickle.dumps({'action': DispatchActions.CHECK, 'data': qid})

    # ------------------------------------------------------------------------
    @staticmethod
    def configure():
        return pickle.dumps({'action': DispatchActions.CONFIGURE})

    # ------------------------------------------------------------------------
    @staticmethod
    def exit(reason: str):
        return pickle.dumps({'action': DispatchActions.EXIT, 'data': reason})

    # ------------------------------------------------------------------------
    @staticmethod
    def fetch(field: str):
        return pickle.dumps({'action': DispatchActions.FETCH, 'data': field})

    # ------------------------------------------------------------------------
    @staticmethod
    def log(level: int, message: str):
        data = {'level': level, 'message': message}
        return pickle.dumps({'action': DispatchActions.LOG, 'data': data})

    # ------------------------------------------------------------------------
    @staticmethod
    def sync(qid: str):
        return pickle.dumps({'action': DispatchActions.SYNC, 'data': qid})

    # ------------------------------------------------------------------------
    @staticmethod
    def remove(qid: str):
        return pickle.dumps({'action': DispatchActions.REMOVE, 'data': qid})

    # ------------------------------------------------------------------------
    @staticmethod
    def state_update(qid: str, input=None, output=None, ray=None, partial=None):
        data = {'qid': qid}
        if input is not None:
            data['input'] = input
        if output is not None:
            data['output'] = output
        if partial is not None:
            data['partial'] = partial
        if ray is not None:
            data['ray'] = ray

        return pickle.dumps({'action': DispatchActions.UPDATE, 'data': data})

    # ------------------------------------------------------------------------
    @staticmethod
    def schema_update(input=None, output=None, config=None):
        data = {}
        if input is not None:
            data['input'] = input
        if output is not None:
            data['output'] = output
        if config is not None:
            data['config'] = config

        return pickle.dumps({'action': DispatchActions.SCHEMA_UPDATE, 'data': data})

    # ------------------------------------------------------------------------
    @staticmethod
    def app_state(state: str):
        return pickle.dumps({'action': DispatchActions.APP_STATE, 'data': state})
