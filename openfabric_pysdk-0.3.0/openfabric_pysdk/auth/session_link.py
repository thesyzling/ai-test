import threading
from typing import Dict, Set

# TODO: Might make sense to be bart of session manager, but at the moment it has nothing in common with it, or authentification
# No need to persist this info on a restart of the app, because the user will have to reconnect(new sockets), and hopefully resume again.

#######################################################
#  SessionLink
#######################################################
class SessionLink:
    __user_sessions: Dict[str, Set[str]]
    __sessions: Set[str]
    __lock: threading.RLock

    # ------------------------------------------------------------------------

    def __init__(self):
        self.__user_sessions: Dict[str, Set[str]] = {}
        self.__sessions: Set[str] = set()
        self.__lock: threading.RLock = threading.RLock()

    # ------------------------------------------------------------------------
    def register_session(self, sid):
        self.__lock.acquire()
        self.__sessions.add(sid)
        self.__lock.release()

    # ------------------------------------------------------------------------
    def unregister_session(self, sid):
        self.__lock.acquire()
        self.__sessions.discard(sid)
        # unregister also user sessions
        users_to_pop = set()
        for uid in self.__user_sessions:
            self.__user_sessions[uid].discard(sid)
            if len(self.__user_sessions[uid]) == 0:
                users_to_pop.add(uid)
        for uid in users_to_pop:
            self.__user_sessions.pop(uid)
        self.__lock.release()

    # ------------------------------------------------------------------------
    def register_user_session(self, uid, sid):
        self.__lock.acquire()
        if uid in self.__user_sessions:
            self.__user_sessions[uid].add(sid)
        else:
            self.__user_sessions[uid] = {sid}
        self.__lock.release()

    # ------------------------------------------------------------------------
    def unregister_user_session(self, uid, sid):
        self.__lock.acquire()
        self.__user_sessions.get(uid, set()).discard(sid)
        if len(self.__user_sessions.get(uid, set())) == 0:
            self.__user_sessions.pop(uid)
        self.__lock.release()

    # ------------------------------------------------------------------------
    
    def get_user_sessions(self, uid):
        self.__lock.acquire()
        try:
            return self.__user_sessions.get(uid, set())
        finally:
            self.__lock.release()

    # ------------------------------------------------------------------------
    
    def get_active_sessions(self):
        self.__lock.acquire()
        try:
            return self.__sessions
        finally:
            self.__lock.release()

session_link = SessionLink()
