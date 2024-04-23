# coding: utf-8
import base64
import codecs
import dataclasses
import hashlib
import json
import logging
import re
import uuid
from typing import Any, Dict, List, NamedTuple, Optional, TYPE_CHECKING, Tuple, Union, cast

if TYPE_CHECKING:
    from wsgiref.types import StartResponse, WSGIEnvironment

import msgpack
import psycopg2
from psycopg2._psycopg import connection

import config
from nktk_raw_pb2 import TrackView

MAX_STORE_SIZE = 1000000


log = logging.getLogger(__name__)
log_level = getattr(logging, config.log['level'])  # type: ignore[arg-type]
log.setLevel(log_level)

log_handler: logging.Handler

log_file = config.log.get('file')
if not log_file:
    log_handler = logging.StreamHandler()
else:
    log_handler = logging.FileHandler(log_file)
log_handler.setLevel(log_level)
log_formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
log_handler.setFormatter(log_formatter)
log.addHandler(log_handler)


_connection: Optional[connection] = None


class TrackRefViewMsgpackEncoded(bytes):
    ...


class ViewDataProtobufEncoded(bytes):
    ...


class GeoDataProtobufEncoded(bytes):
    ...


class TrackViewsNakarteEncoded(bytes):
    ...

class TrackViewTuple(NamedTuple):
    view_data: ViewDataProtobufEncoded
    geodata: GeoDataProtobufEncoded


class TrackRefViewTuple(NamedTuple):
    view_data: ViewDataProtobufEncoded
    geodata_id: int


def check_connection(conn: connection) -> bool:
    try:
        conn.cursor().execute('SELECT 1')
        return True
    except psycopg2.OperationalError:
        return False


def get_connection() -> connection:
    global _connection
    if not _connection or _connection.closed or not check_connection(_connection):
        _connection = psycopg2.connect(
            host=config.db.get('host'),
            port=config.db.get('port'),
            user=config.db.get('user'),
            password=config.db.get('password'),
            dbname=config.db.get('dbname'),
        )
        _connection.set_session(autocommit=True)
    return _connection


def insert_geodata(data: GeoDataProtobufEncoded) -> int:
    table_name = 'geodata'
    connection = get_connection()
    with connection.cursor() as cursor:
        sql = 'INSERT INTO %s (data) VALUES (%%s) ON CONFLICT DO NOTHING RETURNING id' % table_name
        cursor.execute(sql, (data,))
        res = cursor.fetchone()
        if res is None:
            sql = 'SELECT id FROM %s WHERE md5(data)=md5(%%s)' % table_name
            cursor.execute(sql, (data,))
            res = cursor.fetchone()
            assert res
    assert isinstance(res[0], int)
    return res[0]


@dataclasses.dataclass(frozen=True)
class InsertTrackviewResult:
    id: int
    is_new: bool


def insert_trackview(data: TrackRefViewMsgpackEncoded, data_hash: str) -> InsertTrackviewResult:
    table_name = 'trackview'
    connection = get_connection()
    with connection.cursor() as cursor:
        sql = 'INSERT INTO %s (data, hash) VALUES (%%s, %%s) ON CONFLICT DO NOTHING RETURNING id' % table_name
        cursor.execute(sql, (data, data_hash))
        res = cursor.fetchone()
        if res is None:
            is_new = False
            sql = 'SELECT id FROM %s WHERE hash=%%s' % table_name
            cursor.execute(sql, (data_hash,))
            res = cursor.fetchone()
            assert res is not None
        else:
            is_new = True
    return InsertTrackviewResult(id=res[0], is_new=is_new)


def select_geodata(id_: int) -> Optional[GeoDataProtobufEncoded]:
    table_name = 'geodata'
    connection = get_connection()
    with connection.cursor() as cursor:
        sql = 'SELECT data FROM %s WHERE id=%%s' % table_name
        cursor.execute(sql, (id_,))
        res = cursor.fetchone()
        if res:
            assert isinstance(res[0], memoryview)
            return GeoDataProtobufEncoded(res[0])
    return None


@dataclasses.dataclass(frozen=True)
class TrackViewProtobufWithId:
    id: int
    data: TrackRefViewMsgpackEncoded


def select_trackview(data_hash: str) -> Optional[TrackViewProtobufWithId]:
    table_name = 'trackview'
    connection = get_connection()
    with connection.cursor() as cursor:
        sql = 'SELECT id, data FROM %s WHERE hash=%%s' % table_name
        cursor.execute(sql, (data_hash,))
        res = cursor.fetchone()
        if res:
            assert isinstance(res[1], memoryview)
            return TrackViewProtobufWithId(id=res[0], data=TrackRefViewMsgpackEncoded(res[1]))
    return None


def parse_trackviews_from_request(nakarte_track: TrackViewsNakarteEncoded) -> List[TrackViewTuple]:
    result = []
    for part in nakarte_track.split(b'/'):
        if not part:
            continue
        tv = TrackView()
        s = base64.urlsafe_b64decode(part)
        version = s[0] - 64
        if version != 4:
            raise ValueError('Unknown version %s' % version)
        tv.ParseFromString(s[1:])
        result.append(
            TrackViewTuple(
                ViewDataProtobufEncoded(tv.view),
                GeoDataProtobufEncoded(tv.track)),
        )
    return result


def offload_geodata(trackviews: List[TrackViewTuple]) -> List[TrackRefViewTuple]:
    result = []
    for view_data, track_data in trackviews:
        geodata_id = insert_geodata(track_data)
        result.append(TrackRefViewTuple(view_data, geodata_id))
    return result


def serialize_trackviews_for_storage(trackviews: List[TrackRefViewTuple]) -> TrackRefViewMsgpackEncoded:
    return TrackRefViewMsgpackEncoded(msgpack.dumps(trackviews))


def serialize_trackviews_for_response(trackviews: List[TrackViewTuple]) -> TrackViewsNakarteEncoded:
    version = 4
    version_char = bytes([version + 64])
    res = []
    for view_data, geodata in trackviews:
        tv = TrackView()
        tv.view = view_data
        tv.track = geodata
        s = base64.urlsafe_b64encode(version_char + tv.SerializeToString())
        res.append(s)
    return TrackViewsNakarteEncoded(b'/'.join(res))


def parse_trackviews_from_storage(s: TrackRefViewMsgpackEncoded) -> List[TrackRefViewTuple]:
    return cast(List[TrackRefViewTuple], msgpack.loads(s))


def load_geodata(trackviews: List[TrackRefViewTuple]) -> List[TrackViewTuple]:
    result = []
    for view_data, geodata_id in trackviews:
        geodata = select_geodata(geodata_id)
        if geodata is None:
            raise Exception(f'Geodata with id={geodata_id} not found')
        result.append(TrackViewTuple(view_data, geodata))
    return result


def store_track(trackviews: List[TrackViewTuple], data_hash: str) -> InsertTrackviewResult:
    trackrefviews = offload_geodata(trackviews)
    s = serialize_trackviews_for_storage(trackrefviews)
    return insert_trackview(s, data_hash)


@dataclasses.dataclass(frozen=True)
class NakarteTrackWithId:
    id: int
    track: TrackViewsNakarteEncoded


def retrieve_track(data_hash: str) -> Optional[NakarteTrackWithId]:
    res = select_trackview(data_hash)
    if res is None:
        return None
    tracksrefviews = parse_trackviews_from_storage(res.data)
    tracksviews = load_geodata(tracksrefviews)
    s = serialize_trackviews_for_response(tracksviews)
    return NakarteTrackWithId(id=res.id, track=s)


def encode_hash(s: bytes) -> str:
    return base64.urlsafe_b64encode(s).rstrip(b'=').decode('ascii')


def read_log(ip_addr: Optional[str], trackview_id: int) -> None:
    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO read_log (ip_addr, time, trackview_id) VALUES (%s, 'now', %s)", (ip_addr, trackview_id))


def write_log(ip_addr: Optional[str], trackview_id: int) -> None:
    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO write_log (ip_addr, time, trackview_id) VALUES (%s, 'now', %s)",
            (ip_addr, trackview_id))


class Application:
    STATUS_OK = '200 OK'
    STATUS_NOT_FOUND = '404', 'Not Found'
    STATUS_LENGTH_REQUIRED = '411', 'Length Required'
    STATUS_PAYLOAD_TOO_LARGE = '413', 'Payload Too Large'
    STATUS_BAD_REQUEST = '400', 'Bad Request'
    STATUS_INTERNAL_SERVER_ERROR = '500', 'Internal Server Error'

    def __init__(self, environ: Dict[str, Any], start_response: "StartResponse"):
        self.environ = environ
        self._start_response = start_response
        request_id: Optional[str] = environ.get('REQUEST_ID')
        if not request_id:
            request_id = uuid.uuid4().hex
        self.request_id = request_id

    def log(self, level: str, message: str = '', **extra: Union[str, int, Dict[str, str]]) -> None:
        extra = dict(extra, request_id=self.request_id)
        message += ' ' + json.dumps(extra)
        if level == 'EXCEPTION':
            log.exception(message)
        else:
            log.log(getattr(logging, level), message)

    def start_response(self, status: str, headers: List[Tuple[str, str]]) -> None:
        headers = headers[:]
        headers.append(('Access-Control-Allow-Origin', '*'))
        self._start_response(status, headers)

    def handle_store_track(self, request_data_hash: str) -> List[bytes]:
        self.log('INFO', 'Storing track')
        try:
            size = int(self.environ['CONTENT_LENGTH'])
        except (ValueError, KeyError):
            self.log('INFO', 'No content-length')
            return self.error(self.STATUS_LENGTH_REQUIRED)
        if size > MAX_STORE_SIZE:
            self.log('INFO', 'Request content_length too big', max_size=MAX_STORE_SIZE, content_length=size)
            return self.error(self.STATUS_PAYLOAD_TOO_LARGE)
        data: TrackViewsNakarteEncoded = self.environ['wsgi.input'].read(size)
        self.log('INFO', request_body=data.decode('raw_unicode_escape'))
        if len(data) != size:
            self.log('INFO', 'Request body smaller then content-lenght', content_length=size, body_size=len(data))
            return self.error(self.STATUS_BAD_REQUEST)

        if not data:
            self.log('INFO', 'Request body empty')
            return self.error(self.STATUS_BAD_REQUEST)

        data_hash = encode_hash(hashlib.md5(data).digest())
        if data_hash != request_data_hash:
            self.log('INFO', 'Wrong data hash in request', data_hash=data_hash, request_data_hash=request_data_hash)
            return self.error(self.STATUS_BAD_REQUEST)
        try:
            tracks = parse_trackviews_from_request(data)
        except:
            self.log('EXCEPTION', 'Error parsing track from request')
            return self.error(self.STATUS_BAD_REQUEST)
        try:
            res = store_track(tracks, data_hash)
        except:
            self.log('EXCEPTION', 'Error storing track')
            return self.error(self.STATUS_INTERNAL_SERVER_ERROR)
        if res.is_new:
            self.log('INFO', 'Stored new track')
        else:
            self.log('INFO', 'Track found in storage')
        self.log('INFO', 'Success storing track')
        try:
            addr: str = self.environ['REMOTE_ADDR']
            write_log(addr, res.id)
        except:
            self.log('EXCEPTION', 'Error writing write-log')
        self.start_response(self.STATUS_OK, [])
        return [b'']

    def handle_retrieve_track(self, hash: str) -> List[bytes]:
        self.log('INFO', 'Retreiving track')
        try:
            res = retrieve_track(hash)
        except:
            self.log('EXCEPTION', 'Error retrieving track')
            return self.error(self.STATUS_INTERNAL_SERVER_ERROR)
        if res is None:
            self.log('INFO', 'Key not found')
            return self.error(self.STATUS_NOT_FOUND)
        self.log('INFO', 'Success retrieving track')
        try:
            addr: str = self.environ['REMOTE_ADDR']
            read_log(addr, res.id)
        except:
            self.log('EXCEPTION', 'Error writing read-log')
        self.start_response(self.STATUS_OK, [])
        return [bytes(res.track)]

    def error(self, status: Tuple[str, str]) -> List[bytes]:
        self.start_response('%s %s' % status, [])
        message = json.dumps({'requestId': self.request_id, 'status': status[1], 'code': status[0]})
        return [message.encode('utf-8')]

    def get_headers(self) -> Dict[str, str]:
        headers = {}
        for k, v in self.environ.items():
            if k.startswith('HTTP_'):
                k = codecs.unicode_escape_decode(k)[0]
                v = codecs.unicode_escape_decode(v)[0]
                headers[k[5:]] = v
        return headers

    def route(self) -> List[bytes]:
        try:
            method: str = self.environ['REQUEST_METHOD']
            uri: str = self.environ['PATH_INFO']
            addr: str = self.environ['REMOTE_ADDR']
            self.log('INFO', 'Request accepted', method=method, uri=uri, headers=self.get_headers(),
                     remote_addr=addr)
            if method == 'GET':
                m = re.match(r'^/track/([A-Za-z0-9_-]+)$', uri)
                if m:
                    return self.handle_retrieve_track(m.group(1))
            if method == 'POST':
                m = re.match(r'^/track/([A-Za-z0-9_-]+)$', uri)
                if m:
                    return self.handle_store_track(m.group(1))
            self.log('INFO', 'Request did not match any handler')
            return self.error(self.STATUS_NOT_FOUND)
        except Exception:
            try:
                self.log('EXCEPTION')
            except:
                pass
            return self.error(self.STATUS_INTERNAL_SERVER_ERROR)


def application(environ: "WSGIEnvironment", start_response: "StartResponse") -> List[bytes]:
    return Application(environ, start_response).route()


if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    httpd = make_server('localhost', 8080, application)
    httpd.serve_forever()
