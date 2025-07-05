import json
from dataclasses import dataclass, field
from typing import Optional


#######################################################
#  Challenge
#######################################################
@dataclass
class Challenge:

    id: Optional[str] = field(default=None)
    user_id: Optional[str] = field(default=None)
    user_data: Optional[str] = field(default=None)
    app_data: Optional[str] = field(default=None)
    challenge_data: Optional[str] = field(default=None)
    signature: Optional[str] = field(default=None, repr=False)
    request_time: Optional[int] = field(default=None)

    # ------------------------------------------------------------------------
    def matches(self, challenge: 'Challenge') -> bool:
        return (
                challenge is not None and
                self.user_id == challenge.user_id and
                self.user_data == challenge.user_data and
                self.app_data == challenge.app_data and
                self.challenge_data == challenge.challenge_data and
                self.request_time == challenge.request_time
        )

    # ------------------------------------------------------------------------
    def to_json(self) -> str:
        return json.dumps(self, default=lambda o: {k: v for k, v in o.__dict__.items() if k != 'signature'})

    # ------------------------------------------------------------------------
    @staticmethod
    def from_json(data: str) -> 'Challenge':
        data_dict = json.loads(data)
        return Challenge(**data_dict)
