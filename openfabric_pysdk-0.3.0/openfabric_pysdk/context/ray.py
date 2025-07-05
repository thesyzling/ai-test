from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List

from tqdm.asyncio import tqdm, tqdm_asyncio

from openfabric_pysdk.context.bar import Bar
from openfabric_pysdk.context.message import Message, MessageType


#######################################################
#  Ray Status
#######################################################
class RayStatus(Enum):
    QUEUED = 'queued',
    PENDING = 'pending',
    COMPLETED = 'completed',
    RUNNING = 'running',
    CANCELED = 'canceled',
    REMOVED = 'removed',
    UNKNOWN = 'unknown',
    FAILED = 'failed'

    # ------------------------------------------------------------------------
    def __str__(self):
        return self.name


#######################################################
#  Ray
#######################################################
class Ray:
    qid: str
    sid: str
    uid: str
    rid: str
    finished: bool
    bars: Dict[str, Bar]
    status: RayStatus
    created_at: datetime
    updated_at: datetime
    messages: List[Message]
    onchange_callback: Callable[[Any], Any]
    tqdms_info: Dict[str, tqdm_asyncio]

    # ------------------------------------------------------------------------
    def __init__(self, qid):
        # Initialize the attributes through this method to avoid triggering __setattr__
        object.__setattr__(self, 'qid', qid)
        object.__setattr__(self, 'sid', None)
        object.__setattr__(self, 'uid', None)
        object.__setattr__(self, 'rid', None)
        object.__setattr__(self, 'finished', False)
        object.__setattr__(self, 'bars', dict(default=Bar()))
        object.__setattr__(self, 'status', RayStatus.UNKNOWN)
        object.__setattr__(self, 'created_at', datetime.now())
        object.__setattr__(self, 'updated_at', datetime.now())
        object.__setattr__(self, 'messages', list())
        object.__setattr__(self, 'onchange_callback', None)
        object.__setattr__(self, 'tqdms_info', dict())

    # ------------------------------------------------------------------------
    def __str__(self):
        return f"Ray(qid={self.qid}, rid={self.rid}, uid={self.uid}, sid={self.sid}, " + \
            f"status={self.status}, created_at={self.created_at}, updated_at={self.updated_at}, " + \
            f"finished={self.finished}, messages=[{self.messages}]"

    # ------------------------------------------------------------------------
    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key not in ['onchange_callback']:
            self.__trigger_update()

    # ------------------------------------------------------------------------
    def __eq__(self, other):
        if not isinstance(other, Ray):
            return False

        return (
                self.qid == other.qid and
                self.sid == other.sid and
                self.uid == other.uid and
                self.rid == other.rid and
                self.finished == other.finished and
                self.bars == other.bars and
                self.status == other.status and
                self.messages == other.messages
        )

    # ------------------------------------------------------------------------
    # Optional: You can implement __hash__ if Ray objects are used in sets or as dict keys
    def __hash__(self):
        return hash((
            self.qid,
            self.sid,
            self.uid,
            self.rid,
            self.finished,
            frozenset(self.bars.items()),  # Convert dict to frozenset for hashing
            self.status,
            tuple(self.messages),  # Convert list to tuple for hashing
        ))

    # ------------------------------------------------------------------------
    def on_update(self, callback):
        object.__setattr__(self, 'onchange_callback', callback)

    # ------------------------------------------------------------------------
    def update(self, other):
        for name, value in vars(other).items():
            if name in ["qid", "sid", "uid", "rid", "onchange_callback"]:
                continue
            object.__setattr__(self, name, value)
        self.__trigger_update()

    # ------------------------------------------------------------------------
    def complete(self, name='default'):
        bar = self.bars.get(name, Bar())
        self.bars[name] = bar
        bar.remaining = 0
        bar.percent = 100
        object.__setattr__(self, "finished", True)
        self.__trigger_update()

    # ------------------------------------------------------------------------
    def progress(self, name='default', step=1, total=100) -> tqdm_asyncio:
        if self.tqdms_info.get(name) is None:
            self.tqdms_info[name] = tqdm(total=total)
        tqdm_bar = self.tqdms_info[name]
        tqdm_bar.update(step)
        # --
        bar = self.bars.get(name, Bar())
        self.bars[name] = bar
        f_dict = tqdm_bar.format_dict
        rate = f_dict.get("rate")
        total = tqdm_bar.total
        n = tqdm_bar.n
        remaining = (total - n) / rate if rate and total else 0
        bar.remaining = max(0, remaining)
        bar.percent = n
        # --
        self.__trigger_update()
        return tqdm_bar

    # ------------------------------------------------------------------------
    def message(self, message_type: MessageType, content: str):
        message = Message()
        message.type = message_type
        message.content = content
        self.messages.append(message)
        self.__trigger_update()

    # ------------------------------------------------------------------------
    def clear_messages(self):
        if self.messages is not None and len(self.messages) > 0:
            self.messages.clear()
            self.__trigger_update()

    # ------------------------------------------------------------------------
    def tqdms(self):
        return self.tqdms_info

    # ------------------------------------------------------------------------
    def __trigger_update(self):
        object.__setattr__(self, "updated_at", datetime.now())
        if self.onchange_callback is not None:
            self.onchange_callback(self)
