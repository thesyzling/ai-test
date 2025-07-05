import json
import os
import time
import uuid
from threading import Timer
from typing import Any, Callable, Dict, Literal, Optional, Set

from .persistence_service import PersistenceService
from .utils import check_matching_challenge, recover_wallet_address

from openfabric_pysdk.logger import logger


class Config:
    @staticmethod
    def get_owner() -> str:
        return os.getenv("OWNER_WALLET_ADDRESS")


#######################################################
#  SessionManager
#######################################################
class SessionManager:
    config: Config
    locked_app: bool
    authorized_users: Set[str]
    active_tokens: Dict[str, str]
    authorized_sessions: Set[uuid.UUID]
    session_to_user: Dict[uuid.UUID, str]
    active_challenges: Dict[Any, Any]
    persistence_service: PersistenceService

    # ------------------------------------------------------------------------

    def __init__(self, config: Config, persistence_service: PersistenceService):
        self.config = config
        self.persistence_service = persistence_service
        self.authorized_users = set()
        self.authorized_sessions = set()
        self.session_to_user = {}
        self.active_tokens = {}
        self.active_challenges = {}
        self.locked_app = True

        owner = config.get_owner()
        self.locked_app = bool(owner)

        if self.locked_app:
            self.authorized_users.update(persistence_service.get_authorized_users())
            self.authorized_users.add(owner.lower())

            for user_id in self.authorized_users:
                for token in persistence_service.get_user_tokens(user_id):
                    self.active_tokens[token] = user_id

        self.timer = Timer(300, self.cleanup_challenges)
        self.timer.start()

    # ------------------------------------------------------------------------
    def cleanup_challenges(self):
        current_time = time.time() * 1000
        expired_keys = [key for key, value in self.active_challenges.items() if
                        value['requestTime'] + 300000 < current_time]
        for key in expired_keys:
            logger.debug(f"Challenge expired: {key}")
            del self.active_challenges[key]
        self.timer = Timer(300, self.cleanup_challenges)
        self.timer.start()

    # ------------------------------------------------------------------------
    def is_authorized(self, session_id: uuid.UUID) -> bool:
        return not self.locked_app or session_id in self.authorized_sessions

    # ------------------------------------------------------------------------
    def is_owner(self, session_id: uuid.UUID) -> bool:
        return self.locked_app and self.session_to_user.get(session_id) == self.config.get_owner()

    # ------------------------------------------------------------------------
    def get_user(self, session_id: uuid.UUID) -> Optional[str]:
        return self.session_to_user.get(session_id)

    def is_locked(self) -> bool:
        return self.locked_app

    def get_user_by_token(self, token: str) -> Optional[str]:
        return self.active_tokens.get(token)

    def unlink(self, client: uuid.UUID):
        self.session_to_user.pop(client, None)
        self.authorized_sessions.discard(client)

    # ------------------------------------------------------------------------
    def update_user_permissions(self,
                                session_id: uuid.UUID,
                                user_id: str,
                                permission: Literal['grant', 'revoke']) -> Dict[str, str]:
        if not self.is_owner(session_id):
            return {"status": "UNAUTHORIZED", "message": "Only the owner can update permissions"}

        if not user_id:
            return {"status": "BAD_REQUEST", "message": "Invalid user id"}

        user_id = user_id.lower().lstrip("0x")

        if permission == "revoke":
            self.authorized_users.discard(user_id)
            self.active_tokens = {k: v for k, v in self.active_tokens.items() if v != user_id}
        elif permission == "grant":
            self.authorized_users.add(user_id)
            for token in self.persistence_service.get_user_tokens(user_id):
                self.active_tokens[token] = user_id
        else:
            return {"status": "BAD_REQUEST", "message": "Invalid permission"}

        self.persistence_service.set_authorized_users(list(self.authorized_users))
        return {"status": "OK", "message": "Permission updated"}

    # ------------------------------------------------------------------------
    def update_challenge(self, challenge: Dict, error_listener: Callable[[Dict], None]) -> Optional[Dict]:
        logger.debug(f"Challenge: {challenge}")
        if not challenge:
            logger.error("Invalid challenge")
            return None

        current_time = time.time() * 1000
        if not (current_time - 60000 <= challenge['requestTime'] <= current_time + 60000):
            error_listener({"status": "BAD_REQUEST",
                            "message": f"Invalid timestamp. Reference time: {current_time} {challenge['requestTime']}"})
            return None

        if challenge['userId'].lower() not in self.authorized_users:
            logger.error(f"User not authorized: {challenge['userId']}")
            error_listener({"status": "UNAUTHORIZED", "message": "User is not authorized"})
            return None

        signature = challenge['signature']
        del challenge['signature']
        reference = json.dumps(challenge, sort_keys=True, separators=(',', ':'))
        recovered_wallet_address = recover_wallet_address(reference, signature)

        if not challenge['userId'].lower() == recovered_wallet_address.lower():
            logger.error(f"Invalid wallet[{recovered_wallet_address}]. Expecting {challenge['userId']}")
            error_listener({"status": "BAD_REQUEST", "message": "Invalid signature"})
            return None

        challenge['id'] = str(uuid.uuid4())
        challenge['signature'] = None

        self.active_challenges[challenge['id']] = challenge
        return challenge

    # ------------------------------------------------------------------------
    def authorize(self, session_id: uuid.UUID, challenge: Dict[str, Any]) -> Optional[str]:
        if not challenge:
            logger.error("Invalid challenge")
            return None

        active_challenge = self.active_challenges.get(challenge['id'])
        if not active_challenge:
            logger.error("Challenge does not exist")
            return None

        current_time = time.time() * 1000
        if not (active_challenge['requestTime'] - 60000 <= current_time <= active_challenge['requestTime'] + 300000):
            logger.error("Challenge expired")
            return None

        if check_matching_challenge(active_challenge, challenge):
            signature = challenge['signature']
            del challenge['signature']
            reference = json.dumps(challenge, sort_keys=True, separators=(',', ':'))
            recovered_wallet_address = recover_wallet_address(reference, signature)

            if not challenge['userId'].lower() == recovered_wallet_address.lower():
                logger.error(f"Invalid wallet[{recovered_wallet_address}]. Expecting {challenge['userId']}")
                return None

            token = str(uuid.uuid4())
            self.active_tokens[token] = challenge['userId']
            current_tokens = self.persistence_service.get_user_tokens(challenge['userId'])
            current_tokens.append(token)
            self.persistence_service.set_authorized_tokens(challenge['userId'], current_tokens)

            self.session_to_user[session_id] = challenge['userId']
            self.authorized_sessions.add(session_id)

            del self.active_challenges[challenge['id']]

            return token
        else:
            logger.error(f"Challenge mismatch {challenge} {active_challenge}")

        return None

    # ------------------------------------------------------------------------
    def authenticate(self, session_id: uuid.UUID, token: str) -> bool:
        if token in self.active_tokens:
            self.session_to_user[session_id] = self.active_tokens[token]
            self.authorized_sessions.add(session_id)
            return True
        return False


session_manager = SessionManager(Config(), PersistenceService(f"{os.getcwd()}/datastore"))
