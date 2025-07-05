import json
import os
from typing import Any, Dict, List

#######################################################
#  PersistenceService
#######################################################
class PersistenceService:
    config_path: str
    auth_tokens_path: str

    # ------------------------------------------------------------------------
    def __init__(self, config_path: str):
        self.config_path = config_path
        os.makedirs(config_path, exist_ok=True)
        self.auth_tokens_path = os.path.join(config_path, 'tokens.json')
        self._ensure_file_exists(self.auth_tokens_path)

    # ------------------------------------------------------------------------
    def _ensure_file_exists(self, path: str):
        if not os.path.exists(path):
            with open(path, 'w') as file:
                json.dump({}, file)

    # ------------------------------------------------------------------------
    def _read_json(self, path: str):
        with open(path, 'r') as file:
            return json.load(file)

    # ------------------------------------------------------------------------
    def _write_json(self, path: str, data: Dict[Any, Any]):
        with open(path, 'w') as file:
            json.dump(data, file)

    # ------------------------------------------------------------------------
    def get_authorized_users(self) -> List[str]:
        data = self._read_json(self.auth_tokens_path)
        return data.get('authorizeUsers', [])

    # ------------------------------------------------------------------------
    def set_authorized_users(self, authorized_users: List[str]):
        data = self._read_json(self.auth_tokens_path)
        data['authorizeUsers'] = authorized_users
        self._write_json(self.auth_tokens_path, data)

    # ------------------------------------------------------------------------
    def get_user_tokens(self, user_id: str) -> List[str]:
        data = self._read_json(self.auth_tokens_path)
        return data.get(user_id, [])

    # ------------------------------------------------------------------------
    def set_authorized_tokens(self, user_id: str, tokens: List[str]):
        data = self._read_json(self.auth_tokens_path)
        data[user_id] = tokens
        self._write_json(self.auth_tokens_path, data)
