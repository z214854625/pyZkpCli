import argparse
import atexit
import mimetypes

from flask import Flask, jsonify, render_template, request
from kazoo import exceptions as zk_errors
from kazoo.handlers.threading import KazooTimeoutError
from werkzeug.exceptions import HTTPException

from zk_service import (
    CALL_TIMEOUT,
    ApiError,
    ConnectionManager,
    child_path,
    describe_node,
    import_batches,
    import_manifest,
    node_data,
    node_version,
    plan_import,
    search_nodes,
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
        if request.method in {"POST", "PUT", "DELETE"} and request.mimetype != "application/json":
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
    return jsonify(connections.status())


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
    return jsonify(children=[{"name": name, "path": child_path(path, name)} for name in sorted(names)])


@app.get("/api/node")
def get_node():
    connection = connections.require(connection_id())
    path = znode_path(request.args.get("path"))
    data, stat = connection.client.get_async(path).get(CALL_TIMEOUT)
    return jsonify(describe_node(connection, path, data, stat))


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


@app.delete("/api/node")
def delete_node():
    connection = connections.require(connection_id())
    payload = json_object()
    path = znode_path(payload.get("path"))
    if path == "/":
        raise ApiError("protected_path", 403)
    connection.check_write_path(path)
    connection.client.delete_async(path, version=node_version(payload)).get(CALL_TIMEOUT)
    return jsonify(path=path)


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
        try:
            connections.require(connection.id)
            results = transaction.commit_async().get(CALL_TIMEOUT)
        except ApiError as exc:
            raise ApiError(exc.code, exc.status, path=root, created=created, updated=updated) from exc
        except (zk_errors.KazooException, KazooTimeoutError) as exc:
            code, status = zookeeper_error_response(exc, write=True)
            raise ApiError(code, status, path=root, created=created, updated=updated) from exc
        errors = [(index, result) for index, result in enumerate(results) if isinstance(result, Exception)]
        if errors:
            index, error = next(
                ((index, error) for index, error in errors
                 if not isinstance(error, (zk_errors.RolledBackError, zk_errors.RuntimeInconsistency))),
                errors[0],
            )
            code, status = zookeeper_error_response(error, write=True)
            raise ApiError(code, status, path=batch[index].path, created=created, updated=updated)
        created += sum(change.version is None for change in batch)
        updated += sum(change.version is not None for change in batch)
    return jsonify(path=root, created=created, updated=updated, unchanged=len(targets) - len(changes))


@app.get("/api/search")
def search():
    connection = connections.require(connection_id())
    leaves_only = request.args.get("leaves") == "1"
    query = request.args.get("q", "").strip()
    after = request.args.get("after")
    if not query and not leaves_only:
        raise ApiError("empty_search")
    if after is not None:
        after = znode_path(after)
    return jsonify(search_nodes(connection.client, query, leaves_only, after))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local ZooKeeper Web client")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    app.run(host="127.0.0.1", port=args.port, use_reloader=False)
