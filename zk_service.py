import base64
import binascii
import re
import threading
import time
import uuid
from dataclasses import dataclass
from itertools import islice
from typing import Optional
from urllib.parse import urlsplit

from kazoo.client import KazooClient
from kazoo.exceptions import NoAuthError, NoNodeError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.security import CREATOR_ALL_ACL

CALL_TIMEOUT = 5
SEARCH_SCAN_LIMIT = 1000
SEARCH_RESULT_LIMIT = 100
MAX_DATA_BYTES = 1024 * 1024
IMPORT_NODE_LIMIT = 4096
IMPORT_PACKET_LIMIT = MAX_DATA_BYTES - 64
IMPORT_READ_WINDOW = 64
INVALID_PATH_CHAR = re.compile(r"[\x00-\x1f\x7f-\x9f\ud800-\uf8ff\ufff0-\U0010ffff]")
NON_TEXT_CHAR = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]")


class ApiError(Exception):
    def __init__(self, code, status=400, **details):
        super().__init__(code)
        self.code = code
        self.status = status
        self.details = details


def znode_path(value):
    if not isinstance(value, str) or not value.startswith("/"):
        raise ApiError("invalid_path")
    if value != "/" and (
        {"", ".", ".."}.intersection(value[1:].split("/"))
        or INVALID_PATH_CHAR.search(value)
    ):
        raise ApiError("invalid_path")
    return value


def child_path(parent, name):
    return f"{parent.rstrip('/')}/{name}"


def connection_string(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ApiError("invalid_hosts")
    servers, separator, root = value.strip().partition("/")
    endpoints = [endpoint.strip() for endpoint in servers.split(",")]
    for endpoint in endpoints:
        try:
            parsed = urlsplit(f"//{endpoint}")
            if (
                not parsed.hostname
                or parsed.port == 0
                or endpoint.endswith(":")
                or any(c.isspace() or c in "\\@?#" for c in endpoint)
            ):
                raise ValueError()
        except ValueError as exc:
            raise ApiError("invalid_hosts") from exc
    return ",".join(endpoints) + (znode_path(f"/{root}") if separator else "")


@dataclass(frozen=True)
class Connection:
    id: str
    hosts: str
    client: KazooClient
    authenticated: bool

    def is_protected(self, path):
        actual_path = self.client.chroot.rstrip("/") + path
        return actual_path == "/zookeeper" or actual_path.startswith("/zookeeper/")

    def check_write_path(self, path):
        if self.is_protected(path):
            raise ApiError("protected_path", 403)


class ConnectionManager:
    def __init__(self):
        self._active = None
        self._transition = threading.Lock()

    def status(self):
        active = self._active
        if active is None:
            return {"id": None, "state": "disconnected"}
        return {
            "id": active.id,
            "hosts": active.hosts,
            "state": "connected" if active.client.connected else "reconnecting",
            "authenticated": active.authenticated,
        }

    def connect(self, hosts, username, password, expected_id):
        hosts = connection_string(hosts)
        if (
            not isinstance(username, str)
            or not isinstance(password, str)
            or bool(username) != bool(password)
            or ":" in username
        ):
            raise ApiError("invalid_credentials")
        auth_data = [("digest", f"{username}:{password}")] if username else None

        with self._transition:
            self._check_id(self._active, expected_id)
            client = KazooClient(
                hosts=hosts,
                auth_data=auth_data,
                default_acl=CREATOR_ALL_ACL if auth_data else None,
            )
            try:
                client.start_async().wait(timeout=CALL_TIMEOUT)
                if not client.connected:
                    raise KazooTimeoutError()
                if client.chroot and client.exists_async("/").get(CALL_TIMEOUT) is None:
                    raise ApiError("chroot_missing", 404)
            except BaseException:
                close_client(client)
                raise
            self._replace(Connection(uuid.uuid4().hex, hosts, client, bool(auth_data)))
            return self.status()

    def disconnect(self, expected_id):
        with self._transition:
            self._check_id(self._active, expected_id)
            self._replace(None)
            return self.status()

    def close(self):
        with self._transition:
            self._replace(None)

    def _replace(self, connection):
        previous = self._active
        self._active = connection
        if previous is not None:
            close_client(previous.client)

    @staticmethod
    def _check_id(active, expected_id):
        if (active.id if active is not None else None) != expected_id:
            raise ApiError("connection_changed", 409)

    def require(self, expected_id):
        active = self._active
        self._check_id(active, expected_id)
        if active is None or not active.client.connected:
            raise ApiError("connection_unavailable", 503)
        return active


def close_client(client):
    client.stop()
    client.close()


def node_data(payload):
    value = payload.get("value", "")
    encoding = payload.get("encoding", "utf8")
    if not isinstance(value, str) or encoding not in ("utf8", "base64"):
        raise ApiError("invalid_data")
    try:
        if encoding == "base64":
            data = base64.b64decode("".join(value.split()), validate=True)
        else:
            data = value.encode("utf-8")
    except (ValueError, binascii.Error) as exc:
        raise ApiError("invalid_data") from exc
    if len(data) > MAX_DATA_BYTES:
        raise ApiError("data_too_large", 413)
    return data


def node_version(payload):
    version = payload.get("version")
    if type(version) is not int or not 0 <= version < 2**31:
        raise ApiError("invalid_version")
    return version


@dataclass(frozen=True)
class ImportChange:
    path: str
    data: bytes
    version: Optional[int]


def import_packet_size(path, data, prefix_size):
    # Allow for the request headers, path fields, and default ACL.
    return prefix_size + len(path.encode("utf-8")) + len(data or b"") + 64


def import_manifest(connection, payload):
    parent = znode_path(payload.get("parent"))
    name = payload.get("name")
    if not isinstance(name, str) or not name or "/" in name:
        raise ApiError("invalid_name")
    root = znode_path(child_path(parent, name))
    connection.check_write_path(root)
    prefix_size = len(connection.client.chroot.encode("utf-8"))
    if import_packet_size(root, None, prefix_size) > IMPORT_PACKET_LIMIT:
        raise ApiError("invalid_path", path=root)
    directories, files = payload.get("directories"), payload.get("files")
    if not isinstance(directories, list) or not isinstance(files, list):
        raise ApiError("invalid_import")
    if len(directories) + len(files) >= IMPORT_NODE_LIMIT:
        raise ApiError("import_too_many_nodes", 413)

    def destination(relative):
        if not isinstance(relative, str):
            raise ApiError("invalid_import")
        path = child_path(root, relative)
        try:
            znode_path(path)
        except ApiError as exc:
            raise ApiError(exc.code, exc.status, path=path) from exc
        if import_packet_size(path, None, prefix_size) > IMPORT_PACKET_LIMIT:
            raise ApiError("invalid_path", path=path)
        return path

    targets = {root: None}
    for directory in directories:
        targets[destination(directory)] = None
    for entry in files:
        if not isinstance(entry, dict) or "value" not in entry:
            raise ApiError("invalid_import")
        path = destination(entry.get("path"))
        if path in targets:
            raise ApiError("import_path_conflict", 409, path=path)
        try:
            data = node_data({"value": entry["value"], "encoding": "base64"})
        except ApiError as exc:
            code = "import_file_too_large" if exc.status == 413 else exc.code
            raise ApiError(code, exc.status, path=path) from exc
        if import_packet_size(path, data, prefix_size) > IMPORT_PACKET_LIMIT:
            raise ApiError("import_file_too_large", 413, path=path)
        targets[path] = data

    for path in list(targets):
        ancestor = path.rsplit("/", 1)[0] or "/"
        while ancestor != parent:
            if ancestor in targets:
                if targets[ancestor] is not None:
                    raise ApiError("import_path_conflict", 409, path=ancestor)
                break
            if len(targets) >= IMPORT_NODE_LIMIT:
                raise ApiError("import_too_many_nodes", 413)
            targets[ancestor] = None
            ancestor = ancestor.rsplit("/", 1)[0] or "/"
    return parent, root, targets


def import_stat(client, path):
    try:
        return client.exists_async(path).get(CALL_TIMEOUT)
    except NoAuthError as exc:
        raise ApiError("permission_denied", 403, path=path) from exc


def plan_import(client, parent, targets):
    parent_stat = import_stat(client, parent)
    if parent_stat is None:
        raise ApiError("node_missing", 404, path=parent)
    if parent_stat.ephemeralOwner:
        raise ApiError("ephemeral_parent", 409, path=parent)

    changes = []
    # All paths are checked before any transaction. Read ahead to avoid one RTT per file.
    paths = iter(sorted(targets, key=lambda path: (path.count("/"), path)))
    while window := list(islice(paths, IMPORT_READ_WINDOW)):
        reads = [
            client.exists_async(path) if targets[path] is None else client.get_async(path)
            for path in window
        ]
        for path, pending in zip(window, reads):
            data = targets[path]
            current = None
            try:
                result = pending.get(CALL_TIMEOUT)
                if data is None:
                    stat = result
                else:
                    current, stat = result
            except NoNodeError:
                stat = None
            except NoAuthError as exc:
                if data is None:
                    raise ApiError("permission_denied", 403, path=path) from exc
                stat = import_stat(client, path)
            if stat is None:
                changes.append(ImportChange(path, data if data is not None else b"", None))
            elif data is None:
                if stat.ephemeralOwner:
                    raise ApiError("ephemeral_parent", 409, path=path)
            elif stat.numChildren:
                raise ApiError("import_file_has_children", 409, path=path)
            elif current != data:
                changes.append(ImportChange(path, data, stat.version))
    return changes


def import_batches(client, changes):
    batches = []
    batch = []
    batch_size = 0
    prefix_size = len(client.chroot.encode("utf-8"))
    for change in changes:
        size = import_packet_size(change.path, change.data, prefix_size)
        if batch_size + size > IMPORT_PACKET_LIMIT:
            batches.append(batch)
            batch, batch_size = [], 0
        batch.append(change)
        batch_size += size
    if batch:
        batches.append(batch)
    return batches


def describe_node(connection, path, data, stat):
    data = data if data is not None else b""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = None
    if text is None or NON_TEXT_CHAR.search(text):
        encoding = "base64"
        value = base64.b64encode(data).decode("ascii")
    else:
        encoding, value = "utf8", text
    return {
        "path": path,
        "name": path.rsplit("/", 1)[-1] or "/",
        "value": value,
        "encoding": encoding,
        "version": stat.version,
        "data_length": stat.dataLength,
        "num_children": stat.numChildren,
        "created_at": stat.ctime,
        "modified_at": stat.mtime,
        "ephemeral": bool(stat.ephemeralOwner),
        "protected": connection.is_protected(path),
    }


def search_nodes(client, query, leaves_only, after):
    query = query.casefold()
    match_path = "/" in query
    cursor = tuple(after[1:].split("/")) if after and after != "/" else ()
    deadline = time.monotonic() + CALL_TIMEOUT
    stack = [("/", ())]
    results = []
    scanned = inaccessible = 0
    last_visited = after

    # Component order lets a later page resume without walking earlier subtrees.
    while stack and scanned < SEARCH_SCAN_LIMIT and len(results) < SEARCH_RESULT_LIMIT:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        path, components = stack[-1]
        try:
            names = client.get_children_async(path).get(remaining)
            leaf = not names
        except KazooTimeoutError:
            break
        except NoNodeError:
            if path == "/":
                raise
            stack.pop()
            continue
        except NoAuthError:
            names, leaf = (), None
            inaccessible += 1

        stack.pop()
        if after is None or components > cursor:
            scanned += 1
            last_visited = path
            name = components[-1] if components else "/"
            if (not leaves_only or leaf is True) and query in (path if match_path else name).casefold():
                results.append({"path": path, "name": name, "leaf": leaf})

        for name in sorted(names, reverse=True):
            child_components = (*components, name)
            if child_components > cursor or child_components == cursor[: len(child_components)]:
                stack.append((child_path(path, name), child_components))

    if stack and scanned == 0:
        raise KazooTimeoutError()
    return {
        "nodes": results,
        "scanned": scanned,
        "inaccessible": inaccessible,
        "after": last_visited if stack else None,
    }
