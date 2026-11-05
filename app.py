#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import time
import atexit
import logging
import queue
import socket
import threading
import datetime as dt
from collections import deque
from functools import partial
from optparse import OptionParser
from typing import List, Optional

import requests
import libtorrent as lt

from management import CommandDispatcher, LiveHandles, ManagementService, start_management_server

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    create_engine,
    select
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    Session
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

global SESSION
global OPTIONS
global TORRENTS
global LIVE_HANDLES
global TRACKERS
global CONSOLE
global ALIVE
global ALERTS_LOG
global ENGINE
global REMOTE_STORAGE_ENDPOINT

ENGINE = None
REMOTE_STORAGE_ENDPOINT = None
_HTTP_SESSION = None  # lazily initialized requests.Session for connection reuse
METADATA_QUEUE = None
MANAGEMENT_DISPATCHER = None
MANAGEMENT_SERVER = None
LIVE_HANDLES = None


def env_int(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer") from exc


def env_float(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be a number") from exc


def env_list(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return [item.strip() for item in value.split(",") if item.strip()]


DEFAULT_BOOTSTRAP = [
    'router.utorrent.com:6881',
    'router.bitcomet.com:6881',
    'router.bittorrent.com:6881',
    'dht.transmissionbt.com:6881'
]

BOOTSTRAP = env_list("BOOTSTRAP", DEFAULT_BOOTSTRAP)
TORRENT_DIR = os.getenv("TORRENT_DIR", "./torrent")
URL_TRACKER_LIST = os.getenv(
    "URL_TRACKER_LIST",
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt"
)
RESUME_SAVE_TIMEOUT = env_int("RESUME_SAVE_TIMEOUT", 30)
METADATA_QUEUE_SIZE = env_int("METADATA_QUEUE_SIZE", 20)
METADATA_DRAIN_TIMEOUT = env_int("METADATA_DRAIN_TIMEOUT", 30)
MAX_TORRENTS = env_int("MAX_TORRENTS", 100)
TORRENT_TTL = env_int("TORRENT_TTL", 600)
STATUS_RENDER_INTERVAL = env_float("STATUS_RENDER_INTERVAL", 1.0)
STATUS_UPDATE_INTERVAL = env_float("STATUS_UPDATE_INTERVAL", 2.0)


def utc_now():
    """Return naive UTC for compatibility with the existing DateTime columns."""
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def get_http_session():
    global _HTTP_SESSION
    if _HTTP_SESSION is None:
        _HTTP_SESSION = requests.Session()
        atexit.register(_HTTP_SESSION.close)
    return _HTTP_SESSION


def safe_resume_path(name, info_hash):
    """Build a resume path that cannot escape OPTIONS.save_path."""
    safe_name = str(name or "").replace("/", "_").replace("\\", "_")
    safe_name = safe_name.strip().strip(".")[:200] or info_hash
    return os.path.join(OPTIONS.save_path, safe_name + ".fastresume")


class Base(DeclarativeBase):
    pass


class Torrent(Base):
    __tablename__ = "torrents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    info_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)

    name: Mapped[str] = mapped_column(String(512))
    comment: Mapped[Optional[str]] = mapped_column(String(2048), nullable=True)
    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    creation_date: Mapped[Optional[dt.datetime]] = mapped_column(nullable=True)

    total_size: Mapped[int] = mapped_column(BigInteger)
    private: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, default=utc_now
    )
    files: Mapped[List["TorrentFile"]] = relationship(
        back_populates="torrent",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def to_dict(self) -> dict[str, object]:
        return {
            "info_hash": self.info_hash,
            "name": self.name,
            "comment": self.comment,
            "created_by": self.created_by,
            "creation_date": self.creation_date.isoformat() if self.creation_date else None,
            "total_size": self.total_size,
            "private": self.private,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "files": [
                {
                    "file_name": file.file_name,
                    "file_size": file.file_size,
                }
                for file in self.files
            ],
        }


class TorrentFile(Base):
    __tablename__ = "torrent_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    torrent_id: Mapped[int] = mapped_column(
            ForeignKey("torrents.id", ondelete="CASCADE"),
            index=True,
        )

    file_name: Mapped[str] = mapped_column(String(512))
    file_size: Mapped[int] = mapped_column(BigInteger)

    torrent: Mapped["Torrent"] = relationship(back_populates="files")


# -------------------- Console Abstraction --------------------

class WindowsConsole:
    def __init__(self):
        # 依赖外部 Console 模块
        import Console
        self.console = Console.getconsole()

    def clear(self):
        self.console.page()

    def write(self, s):
        self.console.write(s)

    def sleep_and_input(self, seconds):
        import msvcrt
        time.sleep(seconds)
        if msvcrt.kbhit():
            return msvcrt.getch().decode(errors='ignore')
        return None


class UnixConsole:
    def __init__(self):
        if not sys.stdin.isatty():
            self._termios = None
            self._select = None
            self.fd = None
            self.old = None
            return
        import termios
        import select

        self._termios = termios
        self._select = select

        self.fd = sys.stdin
        self.old = termios.tcgetattr(self.fd.fileno())
        new = termios.tcgetattr(self.fd.fileno())
        new[3] = new[3] & ~termios.ICANON
        new[6][termios.VTIME] = 0
        new[6][termios.VMIN] = 1
        termios.tcsetattr(self.fd.fileno(), termios.TCSADRAIN, new)

        atexit.register(self._onexit)

    def _onexit(self):
        if not self._termios or not self.fd:
            return
        self._termios.tcsetattr(self.fd.fileno(), self._termios.TCSADRAIN, self.old)

    def clear(self):
        sys.stdout.write('\033[2J\033[0;0H')
        sys.stdout.flush()

    def write(self, s):
        sys.stdout.write(s)
        sys.stdout.flush()

    def sleep_and_input(self, seconds):
        if not self._select or not self.fd:
            time.sleep(seconds)
            return None
        read, _, _ = self._select.select([self.fd.fileno()], [], [], seconds)
        if read:
            return self.fd.read(1)
        return None


def assemble_torrent_db_object(torrent_info):
    if isinstance(torrent_info, str) and os.path.exists(torrent_info):
        torrent_params = lt.load_torrent_file(torrent_info)
        torrent_info = torrent_params.ti
    if torrent_info.info_hashes().has_v1():
        info_hash = str(torrent_info.info_hashes().v1)
    else:
        info_hash = str(torrent_info.info_hashes().v2)

    name = torrent_info.name()

    total_size = torrent_info.total_size()

    private_flag = False
    try:
        private_flag = torrent_info.priv()
    except AttributeError:
        pass

    creation_date = None
    try:
        ts = torrent_info.creation_date()
        if ts:
            creation_date = dt.datetime.fromtimestamp(
                ts, tz=dt.timezone.utc
            ).replace(tzinfo=None)
    except Exception:
        pass

    files = []
    for file_entry in torrent_info.files():
        # Keep the established API/database unit (whole MiB). Changing this
        # requires an explicit migration for existing rows and remote clients.
        file_size = int(file_entry.size / 1024 / 1024)
        file_name = file_entry.path

        tf = TorrentFile(
            file_name=file_name,
            file_size=file_size
        )
        files.append(tf)

    torrent = Torrent(
        info_hash=info_hash,
        name=name,
        comment=getattr(torrent_info, "comment", lambda: None)() or None,
        created_by=getattr(torrent_info, "creator", lambda: None)() or None,
        creation_date=creation_date,
        total_size=total_size,
        private=private_flag,
        files=files
    )

    return torrent


def save_torrent_db_object(torrent):
    try:
        torrent_db_object = assemble_torrent_db_object(torrent)

        if not OPTIONS.use_local_storage:
            if not REMOTE_STORAGE_ENDPOINT:
                logging.warning("remote storage endpoint missing, skip save")
                return
            try:
                resp = get_http_session().post(
                    REMOTE_STORAGE_ENDPOINT,
                    json=torrent_db_object.to_dict(),
                    timeout=5
                )
                resp.raise_for_status()
            except Exception as e:
                logging.warning("failed to post torrent object: %s", e)
            return

        with Session(ENGINE) as session:
            exists = session.execute(
                select(Torrent).filter_by(info_hash=torrent_db_object.info_hash)
            ).scalar_one_or_none()
            if exists:
                return
            session.add(torrent_db_object)
            session.commit()
    except Exception as e:
        logging.warning("failed to save torrent db object: %s", e)


def get_console():
    return WindowsConsole() if os.name == 'nt' else UnixConsole()


# -------------------- TTL Dict with fixes --------------------

class TTLDict(object):
    """简单 TTL 字典，用于在一定时间内去重 info_hash。"""

    def __init__(self, maxsize: int, ttl: float):
        self.maxsize = maxsize
        self.ttl = ttl
        self._data = {}
        self._lock = threading.Lock()

    def _purge_expired(self):
        now = time.monotonic()
        expired = [k for k, (_, exp) in self._data.items() if exp <= now]
        for k in expired:
            del self._data[k]

    def _get_unexpired(self, key):
        value, expires_at = self._data[key]
        if expires_at <= time.monotonic():
            del self._data[key]
            raise KeyError(key)
        return value

    def __delitem__(self, key):
        with self._lock:
            del self._data[key]

    def put(self, key, value):
        """Set a value, returning False when a new key exceeds capacity."""
        with self._lock:
            self._purge_expired()

            # 已存在则只更新值和过期时间
            if key in self._data:
                self._data[key] = (value, time.monotonic() + self.ttl)
                return True

            # 超过容量则拒绝插入
            if len(self._data) >= self.maxsize:
                return False

            self._data[key] = (value, time.monotonic() + self.ttl)
            return True

    def add_if_absent(self, key, value):
        """Reserve a new key without refreshing or replacing an existing key."""
        with self._lock:
            self._purge_expired()
            if key in self._data or len(self._data) >= self.maxsize:
                return False
            self._data[key] = (value, time.monotonic() + self.ttl)
            return True

    def __setitem__(self, key, value):
        self.put(key, value)

    def __getitem__(self, key):
        with self._lock:
            return self._get_unexpired(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        with self._lock:
            try:
                self._get_unexpired(key)
            except KeyError:
                return False
            return True

    def __len__(self):
        with self._lock:
            self._purge_expired()
            return len(self._data)

    def keys(self):
        with self._lock:
            self._purge_expired()
            return list(self._data.keys())

    def items(self):
        with self._lock:
            self._purge_expired()
            return [(k, v) for k, (v, _) in self._data.items()]

    def values(self):
        with self._lock:
            self._purge_expired()
            return [v for v, _ in self._data.values()]


# -------------------- Formatting helpers --------------------

def add_suffix(val):
    prefix = ['B', 'kB', 'MB', 'GB', 'TB']
    for i, unit in enumerate(prefix):
        if abs(val) < 1000:
            fmt = '%5.3g%s' if i == 0 else '%4.3g%s'
            return fmt % (val, unit)
        val /= 1000
    return '%6.3gPB' % val


def progress_bar(progress, width):
    assert 0 <= progress <= 1
    progress_chars = int(progress * width + 0.5)
    return progress_chars * '#' + (width - progress_chars) * '-'


# -------------------- Printing helpers --------------------

def print_peer_info(peers):
    out = (' down    (total )   up     (total )'
           '  q  r flags  block progress  client\n')

    for p in peers:
        out += '%s/s ' % add_suffix(p.down_speed)
        out += '(%s) ' % add_suffix(p.total_download)
        out += '%s/s ' % add_suffix(p.up_speed)
        out += '(%s) ' % add_suffix(p.total_upload)
        out += '%2d ' % p.download_queue_length
        out += '%2d ' % p.upload_queue_length

        out += 'I' if p.flags & lt.peer_info.interesting else '.'
        out += 'C' if p.flags & lt.peer_info.choked else '.'
        out += 'i' if p.flags & lt.peer_info.remote_interested else '.'
        out += 'c' if p.flags & lt.peer_info.remote_choked else '.'
        out += 'e' if p.flags & lt.peer_info.supports_extensions else '.'
        out += 'l' if p.flags & lt.peer_info.local_connection else 'r'
        out += ' '

        if p.downloading_piece_index >= 0 and p.downloading_total > 0:
            out += progress_bar(float(p.downloading_progress) / p.downloading_total, 15)
        else:
            out += progress_bar(0, 15)
        out += ' '

        if p.flags & lt.peer_info.handshake:
            cid = 'waiting for handshake'
        elif p.flags & lt.peer_info.connecting:
            cid = 'connecting to peer'
        else:
            cid = p.client

        out += '%s\n' % cid[:10]

    CONSOLE.write(out)


def print_download_queue(download_queue):
    out = ""
    for e in download_queue:
        out += '%4d: [' % e['piece_index']
        for b in e['blocks']:
            s = b['state']
            if s == 3:
                out += '#'
            elif s == 2:
                out += '='
            elif s == 1:
                out += '-'
            else:
                out += ' '
        out += ']\n'
    CONSOLE.write(out)


# -------------------- Trackers --------------------

def read_best_trackers():
    trackers = []
    try:
        resp = get_http_session().get(URL_TRACKER_LIST, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logging.warning("failed to fetch trackers list: %s", e)
        return trackers

    for line in resp.text.splitlines():
        tracker = line.strip(" \r")
        if tracker:
            trackers.append(tracker)
    return trackers


# -------------------- Torrent add / settings --------------------

def add_torrent(torrent, *, session=None, registry=None, live_handles=None, options=None, trackers=None):
    """支持 .torrent 路径 / 磁力 / v1 或 v2 info_hash。"""
    session = session if session is not None else SESSION
    registry = registry if registry is not None else TORRENTS
    live_handles = live_handles if live_handles is not None else LIVE_HANDLES
    options = options if options is not None else OPTIONS
    trackers = trackers if trackers is not None else TRACKERS
    tp = lt.add_torrent_params()
    reserved_info_hash = None

    if len(torrent) in {40, 64} and all(c in "0123456789abcdefABCDEF" for c in torrent):
        # 直接 info_hash
        info_hash_str = torrent.lower()
        if live_handles is not None and info_hash_str in live_handles:
            return False
        # Reserve capacity before adding to libtorrent. Assignment cannot
        # report a full TTLDict, so this must use the explicit API.
        if not registry.add_if_absent(info_hash_str, None):
            logging.debug("torrent duplicate or capacity reached: %s", info_hash_str)
            return False
        reserved_info_hash = info_hash_str
        if len(info_hash_str) == 40:
            torrent = f"magnet:?xt=urn:btih:{info_hash_str}"
        else:
            torrent = f"magnet:?xt=urn:btmh:1220{info_hash_str}"
        if options.download_mode and trackers:
            torrent += "&tr=" + "&tr=".join(trackers)

    try:
        if torrent.startswith('magnet:'):
            tp = lt.parse_magnet_uri(torrent)
        else:
            ti = lt.torrent_info(torrent)
            info_hashes = ti.info_hashes()
            if info_hashes.has_v1():
                info_hash = str(info_hashes.v1)
            else:
                info_hash = str(info_hashes.v2)
            resume_file = safe_resume_path(ti.name(), info_hash)
            if os.path.exists(resume_file):
                try:
                    with open(resume_file, 'rb') as f:
                        tp = lt.read_resume_data(f.read())
                except Exception as e:
                    logging.error('failed to read resume file "%s": %s', resume_file, e)
            tp.ti = ti

        if reserved_info_hash is None:
            candidate_hash = get_info_hash(tp.info_hashes)
            if candidate_hash is not None:
                if live_handles is not None and candidate_hash in live_handles:
                    return False
                if not registry.add_if_absent(candidate_hash, None):
                    logging.debug(
                        "torrent duplicate or capacity reached: %s",
                        candidate_hash
                    )
                    return False
                reserved_info_hash = candidate_hash

        tp.save_path = options.save_path
        tp.storage_mode = lt.storage_mode_t.storage_mode_sparse
        tp.flags |= lt.torrent_flags.duplicate_is_error | lt.torrent_flags.paused
        if not options.download_mode:
            tp.flags |= lt.torrent_flags.upload_mode
        session.async_add_torrent(tp)
    except Exception:
        if reserved_info_hash is not None:
            try:
                del registry[reserved_info_hash]
            except KeyError:
                pass
        raise
    return True


def parse_option():
    parser = OptionParser()

    parser.add_option('-p', '--port', type='int', help='set listening port')
    parser.add_option('-i', '--listen-interface', type='string',
                      help='set interface for incoming connections')
    parser.add_option('-o', '--outgoing-interface', type='string',
                      help='set interface for outgoing connections')
    parser.add_option('-d', '--max-download-rate', type='float',
                      help='max download rate in kB/s, 0 means infinite')
    parser.add_option('-u', '--max-upload-rate', type='float',
                      help='max upload rate in kB/s, 0 means infinite')
    parser.add_option('-s', '--save-path', type='string',
                      help='path where the downloaded files should be placed')
    parser.add_option('-r', '--proxy-host', type='string',
                      help="HTTP proxy host and port (host:port)")

    parser.add_option('--print_peer_info', type='int', help='print torrent peer info')
    parser.add_option('--print_download_queue', type='int', help='print torrent download queue')
    parser.add_option('--print_alerts_log', type='int', help='print internal alerts log')
    parser.add_option('--download_mode', type='int', help='download mode (1) or sniff-only (0)')
    parser.add_option('--enable_dht', type='int', help='enable dht (1/0)')
    parser.add_option('--use_local_storage', type='int', help='enable local storage (1/0)')
    parser.add_option(
        '--management-host', type='string',
        help='management HTTP bind address (default: MANAGEMENT_HOST or 127.0.0.1)'
    )
    parser.add_option(
        '--management-port', type='int',
        help='management HTTP port; 0 disables it (default: MANAGEMENT_PORT or 8080)'
    )
    parser.add_option(
        '--management-allow-remote', action='store_true',
        help='explicitly permit a non-loopback management bind'
    )

    parser.set_defaults(
        port=6881,
        listen_interface='0.0.0.0',
        outgoing_interface='',
        max_download_rate=0,
        max_upload_rate=0,
        save_path='./save_path',
        proxy_host='',
        print_peer_info=0,
        print_download_queue=0,
        print_alerts_log=0,
        download_mode=0,
        enable_dht=1,
        use_local_storage=0,
        management_host=os.getenv("MANAGEMENT_HOST", "127.0.0.1"),
        management_port=env_int("MANAGEMENT_PORT", 8080),
        management_allow_remote=False,
    )

    (options, args) = parser.parse_args()

    if options.port < 0 or options.port > 65535:
        options.port = 6881

    options.max_upload_rate *= 1000
    options.max_download_rate *= 1000

    if options.max_upload_rate <= 0:
        options.max_upload_rate = -1
    if options.max_download_rate <= 0:
        options.max_download_rate = -1

    return options, args


def init_settings():
    alert_mask = (
        lt.alert.category_t.error_notification
        | lt.alert.category_t.storage_notification
        | lt.alert.category_t.status_notification
        | lt.alert.category_t.dht_notification
        | lt.alert.category_t.dht_log_notification
    )
    settings = {
        'user_agent': 'python_client/' + lt.__version__,
        'enable_dht': bool(OPTIONS.enable_dht),
        'dht_bootstrap_nodes': ",".join(BOOTSTRAP),
        'listen_interfaces': '%s:%d' % (OPTIONS.listen_interface, OPTIONS.port),
        'download_rate_limit': int(OPTIONS.max_download_rate),
        'upload_rate_limit': int(OPTIONS.max_upload_rate),
        'alert_mask': alert_mask,
        'active_limit': MAX_TORRENTS,
        'connections_limit': 200,
        'outgoing_interfaces': OPTIONS.outgoing_interface,
    }

    if OPTIONS.proxy_host:
        host_port = OPTIONS.proxy_host.split(':', 1)
        if len(host_port) == 2:
            settings['proxy_hostname'] = host_port[0]
            try:
                settings['proxy_port'] = int(host_port[1])
            except ValueError:
                logging.warning("invalid proxy port: %s", host_port[1])
            settings['proxy_type'] = lt.proxy_type_t.http

    return settings


# -------------------- Status & alert helpers --------------------

def check_status():
    if not OPTIONS.download_mode:
        return

    CONSOLE.clear()

    for info_hash, handle in LIVE_HANDLES.items():
        out = ''
        if not handle or not handle.is_valid():
            continue

        status = handle.status()

        out += 'name: %s\n' % status.name

        if status.state != lt.torrent_status.seeding:
            state_str = [
                'queued',
                'checking',
                'downloading metadata',
                'downloading',
                'finished',
                'seeding',
                '',
                'checking fastresume'
            ]

            state_idx = int(status.state)
            if 0 <= state_idx < len(state_str):
                out += state_str[state_idx] + ' '

            out += '%5.4f%% ' % (status.progress * 100)
            out += progress_bar(status.progress, 49)
            out += '\n'

            out += 'total downloaded: %d Bytes\n' % status.total_done
            out += 'peers: %d seeds: %d distributed copies: %d\n' % (
                status.num_peers,
                status.num_seeds,
                getattr(status, 'distributed_copies', 0)
            )
            out += '\n'

        out += 'download: %s/s (%s) ' % (
            add_suffix(status.download_rate),
            add_suffix(status.total_download)
        )
        out += 'upload: %s/s (%s) ' % (
            add_suffix(status.upload_rate),
            add_suffix(status.total_upload)
        )
        out += '\n'

        if status.state != lt.torrent_status.seeding:
            out += 'info-hash: %s\n' % info_hash
            out += 'next announce: %s\n' % status.next_announce
            out += 'tracker: %s\n' % status.current_tracker

        CONSOLE.write(out)

        if OPTIONS.print_peer_info:
            print_peer_info(handle.get_peer_info())
        if OPTIONS.print_download_queue:
            print_download_queue(handle.get_download_queue())

        if status.state != lt.torrent_status.seeding:
            try:
                out = '\n'
                fp = handle.file_progress()
                ti = status.torrent_file
                if ti is not None:
                    fs = ti.files()
                    for idx, p in enumerate(fp):
                        size = float(fs.file_size(idx))
                        if size > 0:
                            prog = p / size
                        else:
                            prog = 0.0
                        out += progress_bar(prog, 20)
                        out += ' ' + fs.file_path(idx) + '\n'
                    CONSOLE.write(out)
            except Exception:
                # 读取 file_progress 过程中有可能抛异常，忽略
                pass

    lines = [
        76 * '-' + '\n',
        '(q)uit, (p)ause, (u)npause, (r)eannounce\n',
        76 * '-' + '\n',
    ]
    for line in lines:
        CONSOLE.write(line)


def persist_torrent_metadata(torrent_file, info_hash):
    """Persist metadata outside the latency-sensitive alert loop."""
    try:
        torrent_obj = lt.create_torrent(torrent_file)
        os.makedirs(TORRENT_DIR, exist_ok=True)
        save_torrent_db_object(torrent_file)

        cache_file = os.path.join(TORRENT_DIR, info_hash + '.torrent')
        with open(cache_file, "wb") as f:
            f.write(lt.bencode(torrent_obj.generate()))

        logging.info("torrent cached: %s", info_hash)
    except Exception as e:
        logging.warning("failed to persist torrent %s: %s", info_hash, e)


def metadata_worker():
    while True:
        torrent_file, info_hash = METADATA_QUEUE.get()
        try:
            persist_torrent_metadata(torrent_file, info_hash)
        finally:
            try:
                del TORRENTS[info_hash]
            except KeyError:
                pass
            METADATA_QUEUE.task_done()


def wait_for_metadata_jobs(timeout=METADATA_DRAIN_TIMEOUT):
    deadline = time.monotonic() + timeout
    while METADATA_QUEUE.unfinished_tasks and time.monotonic() < deadline:
        time.sleep(0.1)
    return METADATA_QUEUE.unfinished_tasks == 0


def handle_alert_metadata_received(a):
    # sniff 模式下只做缓存 .torrent
    if OPTIONS.download_mode or not isinstance(a, lt.metadata_received_alert):
        return

    handle = a.handle
    if not handle.is_valid():
        return

    handle.pause()
    status = handle.status()
    torrent_file = status.torrent_file
    if torrent_file is None:
        return

    try:
        info_hashes = torrent_file.info_hashes()
        info_hash = get_info_hash(info_hashes)
    except Exception as e:
        logging.warning("failed to read torrent info-hash: %s", e)
        info_hash = get_handle_info_hash(handle)

    queued = False
    if info_hash is None:
        logging.warning("dropping metadata with no valid info-hash")
    elif not TORRENTS.put(info_hash, None):
        logging.warning("torrent capacity reached, dropping metadata: %s", info_hash)
    else:
        try:
            METADATA_QUEUE.put_nowait((torrent_file, info_hash))
            queued = True
        except queue.Full:
            logging.warning("metadata queue full, dropping torrent: %s", info_hash)

    # Keep a TTL reservation while the background worker persists this hash.
    if not queued:
        del_handle_torrents(handle, info_hash)
    SESSION.remove_torrent(handle)
    if info_hash is not None and LIVE_HANDLES is not None:
        LIVE_HANDLES.discard(info_hash)


def _seconds_until_next_cleanup(target_hour=3):
    now = dt.datetime.now()
    next_run = now.replace(
        hour=target_hour, minute=0, second=0, microsecond=0
    )
    if next_run <= now:
        next_run += dt.timedelta(days=1)
    return max(1, int((next_run - now).total_seconds()))


def cleanup_old_cache_files(days=7):
    if not os.path.isdir(TORRENT_DIR):
        return
    cutoff = time.time() - days * 24 * 3600
    for entry in os.scandir(TORRENT_DIR):
        if not entry.name.endswith(".torrent"):
            continue
        try:
            if entry.is_file() and entry.stat().st_mtime < cutoff:
                os.remove(entry.path)
        except Exception as e:
            logging.warning("failed to remove cache file %s: %s", entry.path, e)


def cache_cleanup_loop():
    while True:
        time.sleep(_seconds_until_next_cleanup())
        cleanup_old_cache_files()


def handle_alert_announce_peer(a):
    # sniff 模式下才根据 DHT announce 添加种子
    if OPTIONS.download_mode:
        return

    if isinstance(a, lt.dht_pkt_alert):
        try:
            pkt = bytes(a.pkt_buf)
        except Exception:
            return

        try:
            data = lt.bdecode(pkt)
        except Exception:
            return

        if data.get(b'y') == b'q' and data.get(b'q') == b'announce_peer':
            args = data.get(b'a', {})
            info_hash_bytes = args.get(b'info_hash')

            if info_hash_bytes:
                try:
                    info_hash_obj = lt.sha1_hash(info_hash_bytes)
                    info_hash = str(info_hash_obj)
                except Exception:
                    return

                info_hash_file = os.path.join(TORRENT_DIR, info_hash + '.torrent')
                if os.path.exists(info_hash_file):
                    logging.debug("torrent local detected: %s skip", info_hash)
                    return
                if info_hash in TORRENTS:
                    logging.debug("torrent running detected: %s skip", info_hash)
                    return
                logging.info("torrent detected: %s", info_hash)
                add_torrent(info_hash)


def get_info_hash(info_hashes):
    """Return a string info-hash, preferring v1 for DHT compatibility."""
    ihs = info_hashes
    if ihs.has_v1():
        return str(ihs.v1)
    if ihs.has_v2():
        return str(ihs.v2)
    return None


def get_handle_info_hash(handle):
    try:
        if not handle.is_valid():
            return None
        return get_info_hash(handle.status().info_hashes)
    except Exception:
        return None


def add_handle_torrents(handle, info_hash=None):
    info_hash = info_hash or get_handle_info_hash(handle)
    if info_hash is None:
        return False
    if LIVE_HANDLES is not None and not LIVE_HANDLES.put(info_hash, handle):
        return False
    # The TTL cache is only a short-lived deduplication hint. A full or
    # expired cache must never evict a valid handle from the live inventory.
    TORRENTS.put(info_hash, handle)
    return True


def del_handle_torrents(handle, info_hash=None):
    info_hash = info_hash or get_handle_info_hash(handle)
    if info_hash is None:
        return
    if LIVE_HANDLES is not None:
        LIVE_HANDLES.discard(info_hash)
    try:
        del TORRENTS[info_hash]
    except KeyError:
        pass


def handle_alert_state_update(a):
    if isinstance(a, lt.state_update_alert):
        for s in a.status:
            if not s.handle.is_valid():
                continue
            info_hash = get_info_hash(s.info_hashes)
            if not add_handle_torrents(s.handle, info_hash):
                logging.warning("torrent capacity reached, removing %s", info_hash)
                SESSION.remove_torrent(s.handle)


def handle_alert_add_torrent(a):
    if isinstance(a, lt.add_torrent_alert):
        handle = a.handle
        if not handle.is_valid():
            try:
                info_hash = get_info_hash(a.params.info_hashes)
                if info_hash and TORRENTS.get(info_hash) is None:
                    del TORRENTS[info_hash]
            except (AttributeError, KeyError):
                pass
            return
        handle.set_max_connections(60)
        handle.set_max_uploads(-1)
        if not add_handle_torrents(handle):
            logging.warning("torrent capacity reached, removing added torrent")
            SESSION.remove_torrent(handle)


def handle_alert_log(a):
    if OPTIONS.print_alerts_log:
        ALERTS_LOG.append(a.message())


def handle_alerts():
    alerts = SESSION.pop_alerts()
    for a in alerts:
        handle_alert_log(a)
        handle_alert_add_torrent(a)
        handle_alert_state_update(a)
        handle_alert_announce_peer(a)
        handle_alert_metadata_received(a)


def check_console(c):
    global ALIVE

    if not c:
        return

    c = c.lower()
    if c == 'r':
        if MANAGEMENT_DISPATCHER:
            MANAGEMENT_DISPATCHER.service.action("all", "reannounce")
    elif c == 'q':
        ALIVE = False
    elif c == 'p':
        if MANAGEMENT_DISPATCHER:
            MANAGEMENT_DISPATCHER.service.action("all", "pause")
    elif c == 'u':
        if MANAGEMENT_DISPATCHER:
            MANAGEMENT_DISPATCHER.service.action("all", "resume")


def print_alerts_log():
    if not OPTIONS.print_alerts_log:
        return

    alerts = list(ALERTS_LOG)
    ALERTS_LOG.clear()
    for a in alerts:
        CONSOLE.write(a + '\n')


# -------------------- Main alert loop --------------------

def alert_handle():
    global ALIVE
    global ALERTS_LOG

    ALIVE = True
    ALERTS_LOG = deque(maxlen=20)
    next_render = 0.0
    next_status_update = 0.0

    while ALIVE:
        now = time.monotonic()
        if now >= next_render:
            check_status()
            next_render = now + STATUS_RENDER_INTERVAL

        handle_alerts()
        if MANAGEMENT_DISPATCHER:
            MANAGEMENT_DISPATCHER.drain()
        print_alerts_log()

        c = CONSOLE.sleep_and_input(0.5)
        check_console(c)

        if now >= next_status_update:
            # 请求状态更新，用于 state_update_alert
            SESSION.post_torrent_updates()
            next_status_update = now + STATUS_UPDATE_INTERVAL

    # 退出时保存 resume data
    SESSION.pause()
    handles_to_save = {}
    for info_hash, handle in list(LIVE_HANDLES.items()):
        if not handle or not handle.is_valid():
            # Handle invalid / None (already removed from session): drop it now
            # so it doesn't block the drain loop below.
            try:
                del TORRENTS[info_hash]
            except KeyError:
                pass
            LIVE_HANDLES.discard(info_hash)
            continue
        try:
            if not handle.status().has_metadata:
                # No metadata → nothing to resume; drop to avoid blocking exit.
                del_handle_torrents(handle, info_hash)
                continue
        except Exception:
            del_handle_torrents(handle, info_hash)
            continue
        handles_to_save[info_hash] = handle

    pending_resume = set()
    for info_hash, handle in handles_to_save.items():
        try:
            handle.save_resume_data()
        except Exception as e:
            logging.error("failed to request resume data for %s: %s", info_hash, e)
            del_handle_torrents(handle, info_hash)
        else:
            pending_resume.add(info_hash)

    # 等待保存完成
    deadline = time.monotonic() + RESUME_SAVE_TIMEOUT
    while pending_resume and time.monotonic() < deadline:
        alerts = SESSION.pop_alerts()
        for a in alerts:
            if isinstance(a, lt.save_resume_data_alert):
                data = lt.write_resume_data_buf(a.params)
                handle = a.handle
                info_hash = get_handle_info_hash(handle)
                if info_hash in pending_resume:
                    resume_name = handle.status().name or info_hash
                    path = safe_resume_path(resume_name, info_hash)
                    try:
                        with open(path, 'wb') as f:
                            f.write(data)
                    except Exception as e:
                        logging.error("failed to write resume file '%s': %s", path, e)
                    pending_resume.discard(info_hash)
                    del_handle_torrents(handle, info_hash)

            if isinstance(a, lt.save_resume_data_failed_alert):
                handle = a.handle
                info_hash = get_handle_info_hash(handle)
                if info_hash in pending_resume:
                    logging.error('failed to save resume data for %s', handle.status().name)
                    pending_resume.discard(info_hash)
                    del_handle_torrents(handle, info_hash)

        time.sleep(0.5)

    if pending_resume:
        logging.error(
            "timed out waiting for resume data: %s",
            ", ".join(sorted(pending_resume))
        )

    if not wait_for_metadata_jobs():
        logging.error(
            "timed out waiting for %d metadata jobs",
            METADATA_QUEUE.unfinished_tasks
        )


# -------------------- main --------------------

def main():
    global OPTIONS
    global SESSION
    global CONSOLE
    global TRACKERS
    global TORRENTS
    global LIVE_HANDLES
    global ENGINE
    global REMOTE_STORAGE_ENDPOINT
    global METADATA_QUEUE
    global MANAGEMENT_DISPATCHER
    global MANAGEMENT_SERVER

    CONSOLE = get_console()
    OPTIONS, args = parse_option()

    # 确保 save_path 存在
    os.makedirs(OPTIONS.save_path, exist_ok=True)

    # 创建 session（libtorrent 2.x 友好）
    SESSION = lt.session()
    SESSION.apply_settings(init_settings())

    # 创建 db engine（仅当启用本地存储）
    if OPTIONS.use_local_storage:
        ENGINE = create_engine("sqlite:///torrents.db", echo=False, future=True)
        Base.metadata.create_all(ENGINE)
    else:
        REMOTE_STORAGE_ENDPOINT = os.getenv("TORRENT_STORAGE_ENDPOINT", "").strip()
        if not REMOTE_STORAGE_ENDPOINT:
            logging.warning("TORRENT_STORAGE_ENDPOINT is required for remote storage")
            sys.exit()

    # 读取 tracker 列表（失败不会中断）
    TRACKERS = read_best_trackers()

    # 用 TTL 去重 info_hash，并严格限制 libtorrent session 的 torrent 数量
    TORRENTS = TTLDict(maxsize=MAX_TORRENTS, ttl=TORRENT_TTL)
    LIVE_HANDLES = LiveHandles(maxsize=MAX_TORRENTS)

    instance_id = os.getenv("MANAGEMENT_INSTANCE_ID", "").strip() or socket.gethostname()
    management_service = ManagementService(
        SESSION,
        LIVE_HANDLES,
        partial(
            add_torrent,
            session=SESSION,
            registry=TORRENTS,
            live_handles=LIVE_HANDLES,
            options=OPTIONS,
            trackers=TRACKERS,
        ),
        instance_id=instance_id,
        mode="download" if OPTIONS.download_mode else "sniff",
        pending_registry=TORRENTS,
    )
    MANAGEMENT_DISPATCHER = CommandDispatcher(management_service)
    management_token = os.getenv("MANAGEMENT_TOKEN", "")
    try:
        MANAGEMENT_SERVER = start_management_server(
            OPTIONS.management_host,
            OPTIONS.management_port,
            management_token,
            OPTIONS.management_allow_remote,
            MANAGEMENT_DISPATCHER,
        )
    except ValueError as exc:
        logging.error("unsafe management configuration: %s", exc)
        raise SystemExit(2) from exc
    except OSError as exc:
        logging.error("management listener failed: %s", exc)
        raise SystemExit(2) from exc
    if MANAGEMENT_SERVER:
        logging.info(
            "management API listening on %s:%d",
            OPTIONS.management_host,
            OPTIONS.management_port,
        )
        atexit.register(MANAGEMENT_SERVER.server_close)

    # 持久化可能包含网络和磁盘 I/O，使用有界队列避免阻塞 alert 消费。
    METADATA_QUEUE = queue.Queue(maxsize=METADATA_QUEUE_SIZE)
    metadata_thread = threading.Thread(target=metadata_worker, daemon=True)
    metadata_thread.start()

    # 每日清理过期缓存
    cleanup_thread = threading.Thread(target=cache_cleanup_loop, daemon=True)
    cleanup_thread.start()

    # 启动时如果命令行带了 torrent/magnet，就进入下载模式
    for torrent in args:
        OPTIONS.download_mode = 1
        management_service.mode = "download"
        add_torrent(torrent)

    alert_handle()
    if MANAGEMENT_SERVER:
        MANAGEMENT_SERVER.shutdown()
        MANAGEMENT_SERVER.server_close()


if __name__ == '__main__':
    main()
