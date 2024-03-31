from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TrackView(_message.Message):
    __slots__ = ["track", "view"]
    TRACK_FIELD_NUMBER: _ClassVar[int]
    VIEW_FIELD_NUMBER: _ClassVar[int]
    track: bytes
    view: bytes
    def __init__(self, view: _Optional[bytes] = ..., track: _Optional[bytes] = ...) -> None: ...
