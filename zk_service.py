import base64
import binascii
import os
import re
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional
from unicodedata import normalize
from urllib.parse import urlsplit
from zipfile import ZIP_DEFLATED, ZipFile

if os.name == "nt":
    import msvcrt
else:
    import fcntl

from kazoo.client import KazooClient
from kazoo.exceptions import NoAuthError, NoNodeError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.serialization import Create, Delete, multiheader_struct
from kazoo.security import CREATOR_ALL_ACL

CALL_TIMEOUT = 5
CONNECTION_HISTORY_LIMIT = 10
SEARCH_SCAN_LIMIT = 1000
SEARCH_RESULT_LIMIT = 100
MAX_DATA_BYTES = 1024 * 1024
IMPORT_NODE_LIMIT = 4096
TRANSACTION_PACKET_LIMIT = MAX_DATA_BYTES - 64
TREE_READ_WINDOW = 64
TREE_DATA_READ_WINDOW = 8
INVALID_PATH_CHAR = re.compile(r"[\x00-\x1f\x7f-\x9f\ud800-\uf8ff\ufff0-\U0010ffff]")
INVALID_EXPORT_NAME_CHAR = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
RESERVED_EXPORT_NAME = re.compile(r"(?:CON|PRN|AUX|NUL|(?:COM|LPT)[1-9\u00b9\u00b2\u00b3]|CON(?:IN|OUT)\$)")
NON_TEXT_CHAR = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]")
NODE_TEXT_ENCODINGS = {"utf8": "utf-8", "gbk": "gbk"}
NODE_DATA_ENCODINGS = ("base64", *NODE_TEXT_ENCODINGS)


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


def node_name_key(name):
    return name.casefold(), name


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


class ConnectionHistory:
    def __init__(self):
        user_data = os.environ.get("LOCALAPPDATA") or os.environ.get("XDG_DATA_HOME")
        default_dir = Path(user_data or Path.home() / ".local" / "share") / "pyZkpCli"
        self.path = Path(os.environ.get("PYZKPCLI_DATA_DIR") or default_dir) / "connection_history.txt"

    @contextmanager
    def _locked(self):
        # Both launchers use this sidecar so replacing the history does not replace the lock.
        with self.path.with_suffix(".lock").open("a+b") as lock:
            if os.name == "nt":
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
            else:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            yield

    def _read(self):
        recent = []
        try:
            with self.path.open("rb") as file:
                for line in file:
                    try:
                        hosts = connection_string(line.decode("utf-8-sig").rstrip("\r\n"))
                    except (ApiError, UnicodeDecodeError):
                        continue
                    if hosts not in recent:
                        recent.append(hosts)
                        if len(recent) == CONNECTION_HISTORY_LIMIT:
                            break
        except FileNotFoundError:
            pass
        return recent

    def read(self):
        try:
            with self._locked():
                return self._read()
        except FileNotFoundError:
            return []

    def remember(self, hosts):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._locked():
            recent = [hosts]
            recent.extend(entry for entry in self._read() if entry != hosts)
            del recent[CONNECTION_HISTORY_LIMIT:]

            pending = None
            try:
                with NamedTemporaryFile(
                    mode="w", encoding="utf-8", newline="\n", dir=self.path.parent, delete=False
                ) as file:
                    pending = Path(file.name)
                    file.writelines(f"{entry}\n" for entry in recent)
                pending.replace(self.path)
            finally:
                if pending is not None:
                    pending.unlink(missing_ok=True)
            return recent


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
        self._history = ConnectionHistory()

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

    def recent_hosts(self):
        try:
            return self._history.read()
        except OSError:
            return None

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
            status = self.status()
            try:
                status["history"] = self._history.remember(hosts)
            except (OSError, UnicodeError):
                status["history_save_failed"] = True
            return status

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
    if not isinstance(value, str) or encoding not in NODE_DATA_ENCODINGS:
        raise ApiError("invalid_data")
    try:
        if encoding == "base64":
            data = base64.b64decode("".join(value.split()), validate=True)
        else:
            data = value.encode(NODE_TEXT_ENCODINGS[encoding])
    except (ValueError, binascii.Error) as exc:
        raise ApiError("invalid_data") from exc
    if len(data) > MAX_DATA_BYTES:
        raise ApiError("data_too_large", 413)
    return data


def node_data_value(data, encoding):
    if encoding == "base64":
        return base64.b64encode(data).decode("ascii")
    codec = NODE_TEXT_ENCODINGS[encoding]
    try:
        text = data.decode(codec)
        if not NON_TEXT_CHAR.search(text) and text.encode(codec) == data:
            return text
    except UnicodeError:
        pass
    return None


def convert_node_data(payload):
    target = payload.get("target_encoding")
    if target not in NODE_DATA_ENCODINGS:
        raise ApiError("invalid_data")
    value = node_data_value(node_data(payload), target)
    if value is None:
        raise ApiError("invalid_text")
    return value


def node_version(payload):
    version = payload.get("version")
    if type(version) is not int or not 0 <= version < 2**31:
        raise ApiError("invalid_version")
    return version


def read_node_result(read, path):
    try:
        return read.get(CALL_TIMEOUT)
    except NoNodeError as exc:
        raise ApiError("node_missing", 404, path=path) from exc
    except NoAuthError as exc:
        raise ApiError("permission_denied", 403, path=path) from exc
    except KazooTimeoutError as exc:
        raise ApiError("timeout", 504, path=path) from exc


def plan_subtree_delete(client, root, version):
    pending_paths = [root]
    changes = []
    while pending_paths:
        paths = pending_paths[-TREE_READ_WINDOW:]
        del pending_paths[-TREE_READ_WINDOW:]
        reads = [client.get_children_async(path, include_data=True) for path in paths]
        for path, read in zip(paths, reads):
            try:
                names, stat = read.get(CALL_TIMEOUT)
            except NoNodeError as exc:
                raise ApiError("node_missing", 404, path=path) from exc
            except NoAuthError as exc:
                raise ApiError("permission_denied", 403, path=path) from exc
            if path == root and stat.version != version:
                raise ApiError("version_conflict", 409, path=path)
            changes.append((path, stat.version))
            pending_paths.extend(child_path(path, name) for name in names)

    # A parent is visited before its children and must be deleted after them.
    changes.reverse()
    return changes


def subtree_delete_batches(client, changes):
    root, _ = changes[-1]
    prefix_size = len(client.chroot.encode("utf-8"))
    # Each batch includes the root, either to check its version or to delete it.
    limit = TRANSACTION_PACKET_LIMIT - write_operation_size(root, None, prefix_size)
    if limit < 0:
        raise ApiError("delete_path_too_large", 413, path=root)

    batches = []
    batch = []
    batch_size = 0
    for change in islice(changes, len(changes) - 1):
        path, _ = change
        size = write_operation_size(path, None, prefix_size)
        if size > limit:
            raise ApiError("delete_path_too_large", 413, path=path)
        if batch_size + size > limit:
            batches.append(batch)
            batch, batch_size = [], 0
        batch.append(change)
        batch_size += size
    batch.append(changes[-1])
    batches.append(batch)
    return batches


def plan_rename(client, source, destination, version, created_zxid):
    if read_node_result(client.exists_async(destination), destination) is not None:
        raise ApiError("node_exists", 409, path=destination)

    pending = deque([source])
    snapshots = []
    creates = []
    deletes = []
    packet_size = multiheader_struct.size
    chroot = client.chroot.rstrip("/")
    session_id = client.client_id[0]

    while pending:
        paths = [pending.popleft() for _ in range(min(len(pending), TREE_DATA_READ_WINDOW))]
        reads = [
            (client.get_async(path), client.get_children_async(path, include_data=True), client.get_acls_async(path))
            for path in paths
        ]
        for path, (data_read, children_read, acl_read) in zip(paths, reads):
            data, stat = read_node_result(data_read, path)
            if path == source and (stat.version != version or str(stat.czxid) != created_zxid):
                raise ApiError("version_conflict", 409, path=path)
            if stat.ephemeralOwner and stat.ephemeralOwner != session_id:
                raise ApiError("rename_ephemeral", 409, path=path)

            names, children_stat = read_node_result(children_read, path)
            acls, acl_stat = read_node_result(acl_read, path)
            if children_stat != stat or acl_stat != stat:
                raise ApiError("version_conflict", 409, path=path)
            if any(acl.id.scheme == "digest" and acl.id.id.endswith(":x") for acl in acls):
                raise ApiError("rename_acl_masked", 403, path=path)

            target = destination + path[len(source):]
            # Use the wire operations to preserve null data and measure the full request.
            create = Create(chroot + target, data, acls, int(bool(stat.ephemeralOwner)))
            delete = Delete(chroot + path, stat.version)
            packet_size += len(create.serialize()) + len(delete.serialize()) + 2 * multiheader_struct.size
            if packet_size > TRANSACTION_PACKET_LIMIT:
                raise ApiError("rename_too_large", 413, path=source)
            creates.append(create)
            deletes.append(delete)
            snapshots.append((path, target, stat))
            pending.extend(child_path(path, name) for name in names)

    # Recheck identity, children and ACL versions before the versioned deletes.
    remaining = iter(snapshots)
    while window := list(islice(remaining, TREE_READ_WINDOW)):
        reads = [client.exists_async(path) for path, _, _ in window]
        for (path, _, stat), read in zip(window, reads):
            if read_node_result(read, path) != stat:
                raise ApiError("version_conflict", 409, path=path)

    transaction = client.transaction()
    transaction.operations.extend(creates)
    transaction.operations.extend(reversed(deletes))
    operation_paths = [target for _, target, _ in snapshots]
    operation_paths.extend(path for path, _, _ in reversed(snapshots))
    return transaction, operation_paths


@dataclass(frozen=True)
class ImportChange:
    path: str
    data: bytes
    version: Optional[int]


def write_operation_size(path, data, prefix_size):
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
    if write_operation_size(root, None, prefix_size) > TRANSACTION_PACKET_LIMIT:
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
        if write_operation_size(path, None, prefix_size) > TRANSACTION_PACKET_LIMIT:
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
        if write_operation_size(path, data, prefix_size) > TRANSACTION_PACKET_LIMIT:
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
    while window := list(islice(paths, TREE_READ_WINDOW)):
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
        size = write_operation_size(change.path, change.data, prefix_size)
        if batch_size + size > TRANSACTION_PACKET_LIMIT:
            batches.append(batch)
            batch, batch_size = [], 0
        batch.append(change)
        batch_size += size
    if batch:
        batches.append(batch)
    return batches


@dataclass(frozen=True)
class ExportEntry:
    path: str
    relative: str
    directory: bool
    czxid: int


def check_export_name(name, path):
    device = name.split(".", 1)[0].rstrip(" ").upper()
    if (
        not name
        or name.endswith((".", " "))
        or INVALID_EXPORT_NAME_CHAR.search(name)
        or RESERVED_EXPORT_NAME.fullmatch(device)
    ):
        raise ApiError("export_invalid_name", path=path)


def scan_export_tree(client, root, name, check_connection):
    pending = deque([(root, name)])
    while pending:
        check_connection()
        window = [pending.popleft() for _ in range(min(len(pending), TREE_READ_WINDOW))]
        reads = [client.get_children_async(path, include_data=True) for path, _ in window]
        batch = []
        for (path, relative), read in zip(window, reads):
            names, stat = read_node_result(read, path)
            batch.append(ExportEntry(path, relative, bool(names) or path == "/", stat.czxid))
            local_names = set()
            for child in sorted(names, key=node_name_key):
                target = child_path(path, child)
                check_export_name(child, target)
                local_name = normalize("NFC", child).casefold()
                if local_name in local_names:
                    raise ApiError("export_name_conflict", 409, path=target)
                local_names.add(local_name)
                pending.append((target, f"{relative}/{child}"))
        yield batch


def read_export_entries(client, entries, check_connection):
    pending = iter(entries)
    while window := list(islice(pending, TREE_DATA_READ_WINDOW)):
        check_connection()
        reads = [None if entry.directory else client.get_async(entry.path) for entry in window]
        for entry, read in zip(window, reads):
            data = None
            if read is not None:
                data, stat = read_node_result(read, entry.path)
                if stat.numChildren or stat.czxid != entry.czxid:
                    raise ApiError("export_tree_changed", 409, path=entry.path)
                data = data or b""
            yield entry, data


class ExportZipBuffer:
    # ZipFile tracks its own position when writing to an unseekable stream.
    def __init__(self):
        self.buffer = bytearray()

    def write(self, data):
        self.buffer.extend(data)
        return len(data)

    def flush(self):
        pass

    def take(self):
        data = node_data_value(self.buffer, "base64")
        self.buffer.clear()
        return data


def export_tree_events(client, root, check_connection, *, as_zip=False):
    name = root.rsplit("/", 1)[-1] or "zookeeper"
    check_export_name(name, root)
    entries = []
    yield {"type": "scan", "scanned": 0}
    for batch in scan_export_tree(client, root, name, check_connection):
        entries.extend(batch)
        yield {"type": "scan", "scanned": len(entries)}
    yield {"type": "start", "total": len(entries), "name": name}

    output = ExportZipBuffer() if as_zip else None
    with ZipFile(output, "w", compression=ZIP_DEFLATED) if as_zip else nullcontext() as archive:
        for entry, data in read_export_entries(client, entries, check_connection):
            if archive is not None:
                filename = f"{entry.relative}/" if entry.directory else entry.relative
                archive.writestr(filename, data or b"")
                value = output.take()
            else:
                value = None if entry.directory else node_data_value(data, "base64")
            yield {
                "type": "entry", "path": entry.path, "relative": entry.relative,
                "directory": entry.directory, "value": value,
            }
    check_connection()
    yield {"type": "done", "value": output.take() if as_zip else None}


def describe_node(connection, path, data, stat):
    data = data if data is not None else b""
    encoding = "utf8"
    value = node_data_value(data, encoding)
    if value is None and path.lower().endswith(".ini"):
        encoding = "gbk"
        value = node_data_value(data, encoding)
    if value is None:
        encoding = "base64"
        value = node_data_value(data, encoding)
    return {
        "path": path,
        "name": path.rsplit("/", 1)[-1] or "/",
        "value": value,
        "encoding": encoding,
        "version": stat.version,
        "created_zxid": str(stat.czxid),
        "data_length": stat.dataLength,
        "num_children": stat.numChildren,
        "created_at": stat.ctime,
        "modified_at": stat.mtime,
        "ephemeral": bool(stat.ephemeralOwner),
        "protected": connection.is_protected(path),
    }


def search_nodes(client, query, leaves_only, after, *, root="/"):
    query = query.casefold()
    match_path = "/" in query
    root_components = tuple(root[1:].split("/")) if root != "/" else ()
    cursor = tuple(after[1:].split("/")) if after and after != "/" else ()
    if after is not None and cursor[:len(root_components)] != root_components:
        raise ApiError("invalid_path")
    deadline = time.monotonic() + CALL_TIMEOUT
    stack = [(root, root_components)]
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
            if path == root:
                raise
            stack.pop()
            continue
        except NoAuthError:
            names, leaf = (), None
            inaccessible += 1

        stack.pop()
        name = components[-1] if components else "/"
        target = path if match_path else name
        matched = leaf == leaves_only and (not query or query == target.casefold())
        if after is None or components > cursor:
            scanned += 1
            last_visited = path
            if matched:
                result = {"path": path, "name": name, "leaf": leaf}
                if leaves_only:
                    result["sort_name"] = name.casefold()
                results.append(result)

        if matched:
            continue
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
