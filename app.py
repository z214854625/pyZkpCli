import argparse
import atexit
import json
import mimetypes
import socket
import sys
import webbrowser
from threading import Thread

from flask import Flask, Response, jsonify, render_template, request
from kazoo import exceptions as zk_errors
from kazoo.handlers.threading import KazooTimeoutError
from werkzeug.exceptions import HTTPException
from werkzeug.serving import ThreadedWSGIServer

from zk_service import (
    CALL_TIMEOUT,
    ApiError,
    ConnectionManager,
    child_path,
    convert_node_data,
    describe_node,
    export_tree_events,
    import_batches,
    import_manifest,
    node_data,
    node_name_key,
    node_version,
    plan_import,
    plan_rename,
    plan_subtree_delete,
    search_nodes,
    subtree_delete_batches,
    znode_path,
)

# Windows can pick up nonstandard MIME types from the registry.
mimetypes.add_type("text/javascript", ".js")
mimetypes.add_type("text/css", ".css")
mimetypes.add_type("font/woff2", ".woff2")

app = Flask(__name__)
app.config.update(
    TRUSTED_HOSTS=["127.0.0.1", "localhost"],
    MAX_CONTENT_LENGTH=8 * 1024 * 1024,
)
connections = ConnectionManager()
atexit.register(connections.close)

ZK_ERROR_CODES = {
    zk_errors.NoNodeError: ("node_missing", 404),
    zk_errors.NodeExistsError: ("node_exists", 409),
    zk_errors.BadVersionError: ("version_conflict", 409),
    zk_errors.NotEmptyError: ("node_not_empty", 409),
    zk_errors.NoChildrenForEphemeralsError: ("ephemeral_parent", 409),
    zk_errors.NoAuthError: ("permission_denied", 403),
    zk_errors.AuthFailedError: ("authentication_failed", 403),
    zk_errors.BadArgumentsError: ("invalid_path", 400),
    zk_errors.ConnectionLoss: ("connection_unavailable", 503),
    zk_errors.ConnectionClosedError: ("connection_unavailable", 503),
    zk_errors.SessionExpiredError: ("connection_unavailable", 503),
    zk_errors.SessionMovedError: ("connection_unavailable", 503),
    zk_errors.OperationTimeoutError: ("timeout", 504),
}


@app.before_request
def check_origin():
    if request.path.startswith("/api/"):
        origin = request.headers.get("Origin")
        if origin is not None and origin != request.host_url.rstrip("/"):
            raise ApiError("origin_denied", 403)
        if request.method in {"POST", "PUT", "PATCH", "DELETE"} and request.mimetype != "application/json":
            raise ApiError("json_required", 415)


@app.after_request
def response_headers(response):
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
        response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
    return response


@app.errorhandler(ApiError)
def api_error(error):
    return jsonify(error=error.code, **error.details), error.status


def zookeeper_error_response(error, *, write=False):
    if isinstance(error, KazooTimeoutError):
        code, status = "timeout", 504
    else:
        code, status = ZK_ERROR_CODES.get(type(error), ("zookeeper_error", 502))
    if write and status in (503, 504):
        code = "write_unconfirmed"
    return code, status


def commit_transaction(connection, transaction, paths, root, **progress):
    try:
        connections.require(connection.id)
        results = transaction.commit_async().get(CALL_TIMEOUT)
    except ApiError as exc:
        raise ApiError(exc.code, exc.status, path=root, **progress) from exc
    except (zk_errors.KazooException, KazooTimeoutError) as exc:
        code, status = zookeeper_error_response(exc, write=True)
        raise ApiError(code, status, path=root, **progress) from exc

    errors = [(path, result) for path, result in zip(paths, results) if isinstance(result, Exception)]
    if errors:
        path, error = next(
            ((path, error) for path, error in errors
             if not isinstance(error, (zk_errors.RolledBackError, zk_errors.RuntimeInconsistency))),
            errors[0],
        )
        code, status = zookeeper_error_response(error, write=True)
        raise ApiError(code, status, path=path, **progress)


@app.errorhandler(KazooTimeoutError)
@app.errorhandler(zk_errors.KazooException)
def kazoo_error(error):
    code, status = zookeeper_error_response(
        error, write=request.endpoint in {"create_node", "update_node", "delete_node"},
    )
    return jsonify(error=code), status


@app.errorhandler(HTTPException)
def http_error(error):
    if not request.path.startswith("/api/"):
        return error
    if error.code == 413:
        code = "import_request_too_large" if request.endpoint == "upload_tree" else "data_too_large"
    else:
        code = "invalid_request"
    return jsonify(error=code), error.code


def json_object():
    payload = request.get_json()
    if not isinstance(payload, dict):
        raise ApiError("invalid_request")
    return payload


def connection_id():
    return request.headers.get("X-ZK-Connection")


@app.get("/")
def index():
    return render_template("index.html", directory_import=True)


@app.get("/api/connection")
def connection_status():
    return jsonify({**connections.status(), "history": connections.recent_hosts()})


@app.post("/api/connection")
def connect():
    payload = json_object()
    return jsonify(connections.connect(
        payload.get("hosts"), payload.get("username", ""), payload.get("password", ""), connection_id()
    ))


@app.delete("/api/connection")
def disconnect():
    return jsonify(connections.disconnect(connection_id()))


@app.get("/api/children")
def list_children():
    connection = connections.require(connection_id())
    path = znode_path(request.args.get("path", "/"))
    names = connection.client.get_children_async(path).get(CALL_TIMEOUT)
    ordered_names = sorted(names, key=node_name_key)
    return jsonify(children=[{"name": name, "path": child_path(path, name)} for name in ordered_names])


@app.get("/api/node")
def get_node():
    connection = connections.require(connection_id())
    path = znode_path(request.args.get("path"))
    data, stat = connection.client.get_async(path).get(CALL_TIMEOUT)
    return jsonify(describe_node(connection, path, data, stat))


@app.post("/api/node/encoding")
def convert_node_encoding():
    return jsonify(value=convert_node_data(json_object()))


@app.post("/api/node")
def create_node():
    connection = connections.require(connection_id())
    payload = json_object()
    parent = znode_path(payload.get("parent"))
    name = payload.get("name")
    if not isinstance(name, str) or not name or "/" in name:
        raise ApiError("invalid_name")
    path = znode_path(child_path(parent, name))
    connection.check_write_path(path)
    data = node_data(payload)
    created = connection.client.create_async(path, data).get(CALL_TIMEOUT)
    return jsonify(path=created), 201


@app.put("/api/node")
def update_node():
    connection = connections.require(connection_id())
    payload = json_object()
    path = znode_path(payload.get("path"))
    connection.check_write_path(path)
    data = node_data(payload)
    stat = connection.client.set_async(path, data, version=node_version(payload)).get(CALL_TIMEOUT)
    return jsonify(describe_node(connection, path, data, stat))


@app.patch("/api/node")
def rename_node():
    connection = connections.require(connection_id())
    payload = json_object()
    path = znode_path(payload.get("path"))
    if path == "/":
        raise ApiError("protected_path", 403)
    connection.check_write_path(path)
    name = payload.get("name")
    if not isinstance(name, str) or not name or "/" in name:
        raise ApiError("invalid_name")
    parent = path.rsplit("/", 1)[0] or "/"
    destination = znode_path(child_path(parent, name))
    connection.check_write_path(destination)
    if destination == path:
        raise ApiError("rename_same_name", 409)
    version = node_version(payload)
    created_zxid = payload.get("created_zxid")
    if not isinstance(created_zxid, str):
        raise ApiError("invalid_request")

    try:
        transaction, paths = plan_rename(connection.client, path, destination, version, created_zxid)
    except (zk_errors.KazooException, KazooTimeoutError) as exc:
        code, status = zookeeper_error_response(exc)
        raise ApiError(code, status, path=path) from exc
    commit_transaction(connection, transaction, paths, path)
    return jsonify(path=destination)


@app.delete("/api/node")
def delete_node():
    connection = connections.require(connection_id())
    payload = json_object()
    path = znode_path(payload.get("path"))
    if path == "/":
        raise ApiError("protected_path", 403)
    connection.check_write_path(path)
    version = node_version(payload)
    recursive = payload.get("recursive", False)
    if type(recursive) is not bool:
        raise ApiError("invalid_request")
    client = connection.client
    if not recursive:
        client.delete_async(path, version=version).get(CALL_TIMEOUT)
        return jsonify(path=path)

    try:
        changes = plan_subtree_delete(client, path, version)
    except (zk_errors.KazooException, KazooTimeoutError) as exc:
        code, status = zookeeper_error_response(exc)
        raise ApiError(code, status, path=path) from exc

    deleted = 0
    for batch in subtree_delete_batches(client, changes):
        transaction = client.transaction()
        operation_paths = []
        if batch[-1][0] != path:
            transaction.check(path, version)
            operation_paths.append(path)
        for target, expected_version in batch:
            transaction.delete(target, version=expected_version)
            operation_paths.append(target)
        commit_transaction(connection, transaction, operation_paths, path, deleted=deleted)
        deleted += len(batch)
    return jsonify(path=path, deleted=deleted)


@app.post("/api/import")
def upload_tree():
    connection = connections.require(connection_id())
    parent, root, targets = import_manifest(connection, json_object())
    changes = plan_import(connection.client, parent, targets)
    batches = import_batches(connection.client, changes)
    created = updated = 0
    for batch in batches:
        transaction = connection.client.transaction()
        for change in batch:
            if change.version is None:
                transaction.create(change.path, change.data)
            else:
                transaction.set_data(change.path, change.data, version=change.version)
        commit_transaction(
            connection, transaction, [change.path for change in batch], root,
            created=created, updated=updated,
        )
        created += sum(change.version is None for change in batch)
        updated += sum(change.version is not None for change in batch)
    return jsonify(path=root, created=created, updated=updated, unchanged=len(targets) - len(changes))


@app.get("/api/export")
def download_tree():
    connection = connections.require(connection_id())
    path = znode_path(request.args.get("path"))
    as_zip = request.args.get("zip") == "1"

    def check_connection():
        connections.require(connection.id)

    def stream():
        try:
            for event in export_tree_events(connection.client, path, check_connection, as_zip=as_zip):
                yield json.dumps(event, separators=(",", ":")) + "\n"
        except ApiError as error:
            yield json.dumps({"error": error.code, **error.details}) + "\n"
        except (zk_errors.KazooException, KazooTimeoutError) as error:
            code, _ = zookeeper_error_response(error)
            yield json.dumps({"error": code, "path": path}) + "\n"

    return Response(stream(), mimetype="application/x-ndjson")


@app.get("/api/search")
def search():
    connection = connections.require(connection_id())
    leaves_only = request.args.get("leaves") == "1"
    query = request.args.get("q", "").strip()
    after = request.args.get("after")
    if not query and not leaves_only:
        raise ApiError("empty_search")
    root = znode_path(request.args.get("root", "/"))
    if after is not None:
        after = znode_path(after)
    return jsonify(search_nodes(connection.client, query, leaves_only, after, root=root))


class LocalHTTPServer(ThreadedWSGIServer):
    allow_reuse_address = not hasattr(socket, "SO_EXCLUSIVEADDRUSE")

    def server_bind(self):
        # Windows SO_REUSEADDR can let two instances listen on the same port.
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def main():
    packaged = getattr(sys, "frozen", False)
    parser = argparse.ArgumentParser(description="Local ZooKeeper Web client")
    parser.add_argument(
        "--port", type=int, default=0 if packaged else 8765,
        help="local port (default: %(default)s; 0 selects a free port)",
    )
    if packaged:
        parser.add_argument("--no-browser", action="store_true", help="skip opening the browser")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")

    host = "127.0.0.1"
    with LocalHTTPServer(host, args.port, app) as server:
        url = f"http://{host}:{server.server_port}/"
        print(f"Running on {url}", flush=True)
        print("Press Ctrl+C to stop.", flush=True)
        if packaged and not args.no_browser:
            Thread(target=webbrowser.open, args=(url,), daemon=True).start()
        server.serve_forever()


if __name__ == "__main__":
    main()
