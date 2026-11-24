"use strict";

const $ = (id) => document.getElementById(id);
const connectionDialog = $("connection-dialog");
const createDialog = $("create-dialog");
const importDialog = $("import-dialog");
const deleteDialog = $("delete-dialog");
const editor = $("editor");
const valueInput = $("node-value");
const navigation = $("navigation");
const entries = new Map();
const dateFormat = new Intl.DateTimeFormat("zh-CN", {
  dateStyle: "medium",
  timeStyle: "medium",
});

const messages = {
  invalid_hosts: "请输入有效的 ZooKeeper 地址，例如 127.0.0.1:2181。",
  invalid_credentials: "Digest 认证需要同时填写用户名和密码，用户名不能包含冒号。",
  chroot_missing: "指定的 ZooKeeper 根路径不存在。",
  connection_changed: "服务器连接已切换，请重新选取节点。",
  connection_unavailable: "ZooKeeper 暂时不可用，请检查连接。",
  node_missing: "节点已不存在。",
  node_exists: "父节点下已有同名节点。",
  node_not_empty: "节点仍有子节点，请先删除子节点。",
  ephemeral_parent: "临时节点不能创建子节点。",
  version_conflict: "节点数据已被修改，请重新读取节点后再试。",
  permission_denied: "当前 ZooKeeper 身份无权执行此操作。",
  authentication_failed: "ZooKeeper 认证失败。",
  protected_path: "此节点受到保护，无法执行该操作。",
  invalid_name: "请输入单层节点名，名称不能包含 /。",
  invalid_path: "节点路径不符合 ZooKeeper 格式。",
  invalid_data: "数据格式无效，请检查 UTF-8 或 Base64 内容。",
  invalid_version: "节点版本无效，请重新读取节点。",
  data_too_large: "数据过大，最多支持 1 MiB。",
  timeout: "ZooKeeper 请求超时，请稍后重试。",
  write_unconfirmed: "未收到写入结果，请重新读取节点确认服务器状态。",
  invalid_import: "目录结构无效，请重新选择目录。",
  import_path_conflict: "目录和文件不能占用同一个节点路径。",
  import_file_has_children: "这个文件对应的节点已有子节点，不能作为文件叶节点导入。",
  import_request_too_large: "本次上传超过 8 MiB 请求上限，请缩小所选目录。",
  import_file_too_large: "该文件与对应路径超过 ZooKeeper 单个请求的大小上限。",
  import_too_many_nodes: "一个目录最多导入 4096 个节点。",
  network_error: "无法访问本地 Web 服务。",
};

let connection = { id: null, state: "disconnected" };
let connectionBusy = false;
let statusRequest = 0;
let selectedPath = null;
let detailRequest = 0;
let node = null;
let editingEncoding = "utf8";
let originalValue = "";
let draftValue = "";
let lineEndingSource = "";
let mutationBusy = false;
let directoryImport = null;
let searchState = null;
let notificationTimer;

function icon(name) {
  const element = document.createElement("i");
  element.className = `icon icon-${name}`;
  element.setAttribute("aria-hidden", "true");
  return element;
}

function messageFor(error) {
  return messages[error.message] ?? "操作失败，请检查 ZooKeeper 服务后重试。";
}

async function api(route, { method = "GET", parameters, body } = {}) {
  const query = parameters ? `?${new URLSearchParams(parameters)}` : "";
  const headers = { Accept: "application/json" };
  if (connection.id) headers["X-ZK-Connection"] = connection.id;
  if (method !== "GET") headers["Content-Type"] = "application/json";
  let response;
  try {
    response = await fetch(`/api/${route}${query}`, {
      method,
      headers,
      body: method === "GET" ? undefined : JSON.stringify(body ?? {}),
      cache: "no-store",
    });
  } catch {
    throw new Error(method === "GET" || route === "connection" ? "network_error" : "write_unconfirmed");
  }
  const data = await response.json();
  if (!response.ok) {
    if (data.error === "connection_changed") void pollStatus();
    const error = new Error(data.error);
    error.details = data;
    throw error;
  }
  return data;
}

function setMessage(element, message) {
  element.textContent = message;
  element.hidden = !message;
}

function notify(message, error = false) {
  const element = $("notification");
  clearTimeout(notificationTimer);
  element.dataset.error = error;
  setMessage(element, message);
  notificationTimer = setTimeout(() => setMessage(element, ""), error ? 6500 : 3000);
}

function connected() {
  return connection.state === "connected";
}

function dirty() {
  return node !== null && originalValue !== draftValue;
}

function allowDiscard() {
  return !dirty() || window.confirm("放弃未保存的节点数据修改？");
}

function updateControls() {
  const canRead = connected() && !connectionBusy && !mutationBusy;
  const canEdit = canRead && node !== null && !node.protected;
  $("open-connection").disabled = connectionBusy || mutationBusy;
  $("refresh-tree").disabled = !canRead;
  $("refresh-node").disabled = !canRead;
  $("retry-detail").disabled = !canRead;
  $("locate-node").disabled = !canRead;
  $("query").disabled = !canRead;
  $("leaves").disabled = !canRead;
  $("submit-search").disabled = !canRead || !($("query").value.trim() || $("leaves").checked);
  $("open-create").disabled = !canEdit || node.ephemeral;
  $("open-delete").disabled = !canEdit || node.path === "/" || node.num_children > 0;
  $("open-delete").title = node?.num_children ? "请先删除子节点" : "删除节点";
  valueInput.readOnly = !canEdit;
  for (const input of editor.elements["node-encoding"]) input.disabled = !canRead;
  $("save-node").disabled = !canEdit || !dirty();
  $("reset-node").disabled = !canEdit || !dirty();
  if (importDialog) {
    $("open-import").disabled = !canEdit || node.ephemeral;
    $("import-submit").disabled = !canRead || !directoryImport?.source || !importDialog.open
      || importDialog.dataset.busy === "true" || !$("import-parent").value.startsWith("/");
  }
}

function showEmpty(title, iconName, message = "", retry = false) {
  node = null;
  $("node-detail").hidden = true;
  $("empty-icon").replaceChildren(icon(iconName));
  $("empty-heading").textContent = title;
  setMessage($("empty-message"), message);
  $("retry-detail").hidden = !retry;
  $("empty-detail").hidden = false;
  updateControls();
}

function applyConnection(next) {
  const changed = next.id !== connection.id;
  const wasConnected = connected();
  connection = next;
  const state = $("connection-status");
  state.dataset.state = connection.state;
  state.textContent = { connected: "已连接", reconnecting: "重连中", disconnected: "未连接" }[connection.state];
  setMessage($("connection-hosts"), connection.hosts ?? "");
  $("connection-hosts").title = connection.hosts ?? "";
  $("connection-caption").textContent = connection.id ? "连接设置" : "连接服务器";
  if (changed) {
    if (dirty()) notify("连接已切换，当前编辑已结束。", true);
    for (const dialog of [createDialog, importDialog, deleteDialog]) if (dialog?.open) dialog.close();
    entries.clear();
    detailRequest += 1;
    selectedPath = null;
    clearSearch();
    showEmpty(connection.id ? "等待连接" : "尚未连接", "plug");
  }
  if (connected() && !selectedPath) {
    const root = treeEntry({ name: "/", path: "/" });
    root.open = true;
    void loadChildren(root);
    void selectNode("/");
  } else if (connected() && !wasConnected) {
    for (const entry of entries.values()) {
      if (entry.open && entry.children === null) void loadChildren(entry);
    }
    if (node === null && selectedPath) void selectNode(selectedPath);
  }
  updateControls();
}

async function pollStatus() {
  if (connectionBusy) return;
  const request = ++statusRequest;
  try {
    const next = await api("connection");
    if (request === statusRequest) applyConnection(next);
  } catch {
    if (request === statusRequest && connection.id) applyConnection({ ...connection, state: "reconnecting" });
  }
}

function treeEntry(item) {
  if (!entries.has(item.path)) {
    entries.set(item.path, { ...item, open: false, children: null, error: "", loading: false, request: 0 });
  }
  return entries.get(item.path);
}

async function loadChildren(entry) {
  const request = ++entry.request;
  entry.loading = true;
  entry.error = "";
  renderNavigation();
  try {
    const { children } = await api("children", { parameters: { path: entry.path } });
    if (request !== entry.request || entries.get(entry.path) !== entry) return;
    entry.children = children.map(treeEntry);
  } catch (error) {
    if (request !== entry.request || entries.get(entry.path) !== entry) return;
    entry.error = messageFor(error);
  } finally {
    if (request === entry.request && entries.get(entry.path) === entry) {
      entry.loading = false;
      renderNavigation();
    }
  }
}

function treeMessage(text) {
  const item = document.createElement("li");
  item.className = "tree-message";
  item.textContent = text;
  return item;
}

function list() {
  const element = document.createElement("ul");
  element.className = "navigation-list";
  return element;
}

function treeItem(entry) {
  const item = document.createElement("li");
  const row = document.createElement("div");
  row.className = "tree-row";
  row.classList.toggle("selected", entry.path === selectedPath);
  const leaf = (entry.children?.length ?? (node?.path === entry.path ? node.num_children : null)) === 0;
  if (leaf && entry.path !== "/") {
    const spacer = document.createElement("span");
    spacer.className = "tree-spacer";
    row.append(spacer);
  } else {
    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "icon-button";
    toggle.title = entry.open ? "折叠子节点" : "展开子节点";
    toggle.setAttribute("aria-label", `${toggle.title} ${entry.path}`);
    toggle.setAttribute("aria-expanded", entry.open);
    toggle.dataset.action = "toggle";
    toggle.dataset.path = entry.path;
    toggle.append(icon(entry.open ? "chevron-down" : "chevron-right"));
    row.append(toggle);
  }
  const button = document.createElement("button");
  const label = document.createElement("span");
  label.className = "tree-label";
  label.textContent = entry.path === "/" ? "/  根节点" : entry.name;
  button.type = "button";
  button.className = "tree-select";
  button.title = entry.path;
  button.dataset.action = "select";
  button.dataset.path = entry.path;
  if (entry.path === selectedPath) button.setAttribute("aria-current", "true");
  button.append(icon(leaf && entry.path !== "/" ? "file" : entry.open ? "folder-open" : "folder"), label);
  row.append(button);
  item.append(row);
  if (entry.open && (entry.loading || entry.error || entry.children?.length)) {
    const children = list();
    if (entry.loading) children.append(treeMessage("正在加载…"));
    if (entry.error) children.append(treeMessage(entry.error));
    for (const child of entry.children ?? []) children.append(treeItem(child));
    item.append(children);
  }
  return item;
}

function searchItem(result) {
  const item = document.createElement("li");
  const button = document.createElement("button");
  const content = document.createElement("span");
  const name = document.createElement("span");
  const path = document.createElement("span");
  button.type = "button";
  button.className = "result";
  button.classList.toggle("selected", result.path === selectedPath);
  if (result.path === selectedPath) button.setAttribute("aria-current", "true");
  button.dataset.action = "select";
  button.dataset.path = result.path;
  content.className = "result-text";
  name.className = "result-name";
  name.textContent = result.name;
  path.className = "result-path mono";
  path.textContent = result.path;
  content.append(name, path);
  button.append(icon(result.leaf === null ? "lock-keyhole" : result.leaf ? "file" : "folder"), content);
  item.append(button);
  return item;
}

function renderNavigation() {
  const focus = document.activeElement;
  const hadFocus = navigation.contains(focus) && focus.dataset.action;
  const tree = list();
  $("navigation-label").textContent = searchState ? "搜索结果" : "节点树";
  $("navigation-count").textContent = searchState ? `${searchState.results.length} 项` : "";
  const root = entries.get("/");
  if (searchState) {
    for (const result of searchState.results) tree.append(searchItem(result));
    if (searchState.loading) tree.append(treeMessage("正在搜索…"));
    else if (searchState.error) tree.append(treeMessage(searchState.error));
    else if (!searchState.results.length && !searchState.after) tree.append(treeMessage("未找到匹配节点"));
    if (searchState.inaccessible) {
      tree.append(treeMessage(`${searchState.inaccessible} 个节点的子节点列表无法访问`));
    }
  } else if (root) {
    tree.append(treeItem(root));
  }
  navigation.replaceChildren(tree);
  if (searchState && !searchState.loading && searchState.after) {
    const more = document.createElement("button");
    more.type = "button";
    more.className = "button more-results";
    more.dataset.action = "more";
    more.textContent = "继续搜索";
    navigation.append(more);
  }
  if (hadFocus) {
    for (const button of navigation.querySelectorAll("button[data-action]")) {
      if (button.dataset.action === focus.dataset.action && button.dataset.path === focus.dataset.path) {
        button.focus({ preventScroll: true });
        break;
      }
    }
  }
}

function clearSearch() {
  searchState = null;
  $("query").value = "";
  $("leaves").checked = false;
  $("clear-search").hidden = true;
  renderNavigation();
  updateControls();
}

async function runSearch(append = false) {
  if (!connected() || mutationBusy) return;
  const q = $("query").value.trim();
  const leaves = $("leaves").checked ? "1" : "0";
  if (!q && leaves === "0") {
    clearSearch();
    return;
  }
  if (!append) searchState = { results: [], after: null, inaccessible: 0, q, leaves };
  const search = searchState;
  search.loading = true;
  search.error = "";
  $("clear-search").hidden = false;
  renderNavigation();
  try {
    const parameters = { q: search.q, leaves: search.leaves };
    if (search.after !== null) parameters.after = search.after;
    const results = await api("search", { parameters });
    search.results.push(...results.nodes);
    search.after = results.after;
    search.inaccessible += results.inaccessible;
  } catch (error) {
    search.error = messageFor(error);
  } finally {
    search.loading = false;
    if (searchState === search) renderNavigation();
  }
}

async function locatePath(path, { refreshParent = false, expandTarget = false } = {}) {
  const expectedId = connection.id;
  clearSearch();
  let entry = entries.get("/");
  const parts = path === "/" ? [] : path.slice(1).split("/");
  for (const [index, part] of parts.entries()) {
    if (connection.id !== expectedId) return;
    if (!entry) break;
    entry.open = true;
    let child = entry.children?.find((item) => item.name === part);
    if (!child || (refreshParent && index === parts.length - 1)) {
      await loadChildren(entry);
      child = entry.children?.find((item) => item.name === part);
    }
    entry = child;
  }
  if (connection.id !== expectedId) return;
  renderNavigation();
  if (!entry) {
    notify("当前树中无法定位该节点。", true);
    return;
  }
  if (expandTarget) {
    entry.open = true;
    await loadChildren(entry);
  }
  if (connection.id !== expectedId) return;
  const selected = [...navigation.querySelectorAll(".tree-select")].find((el) => el.dataset.path === path);
  selected?.scrollIntoView({ block: "nearest" });
}

function setEncoding(encoding) {
  editingEncoding = encoding;
  editor.elements["node-encoding"].value = encoding;
}

function setEditorValue(value) {
  valueInput.value = value;
  draftValue = value;
  lineEndingSource = editingEncoding === "utf8" && value.includes("\r") ? value : "";
}

function restoreLineEndings(value, source) {
  const lineBreak = /\r\n?|\n/g;
  const endings = source.match(lineBreak);
  const fallback = endings[0];
  if (endings.every((ending) => ending === fallback)) return value.replace(/\n/g, fallback);

  // Keep the endings of matching lines on both sides of edits in mixed files.
  const oldLines = source.split(lineBreak);
  const newLines = value.split("\n");
  let oldEnd = endings.length;
  let newEnd = newLines.length - 1;
  let start = 0;
  while (start < oldEnd && start < newEnd && oldLines[start] === newLines[start]) start += 1;
  while (oldEnd > start && newEnd > start && oldLines[oldEnd] === newLines[newEnd]) {
    oldEnd -= 1;
    newEnd -= 1;
  }
  let line = 0;
  return value.replace(/\n/g, () => {
    const index = line++;
    if (index >= newEnd) return endings[oldEnd + index - newEnd];
    return index < oldEnd ? endings[index] : fallback;
  });
}

function renderNode() {
  if (node.encoding === "base64") {
    try {
      node.value = convertEncoding(node.value, "base64", "utf8");
      node.encoding = "utf8";
    } catch {
      // Older running backends can report CRLF text as Base64.
    }
  }
  $("empty-detail").hidden = true;
  $("node-detail").hidden = false;
  const root = node.path === "/";
  $("node-name").textContent = root ? "根节点" : node.name;
  $("node-kind").replaceChildren(icon(node.protected ? "lock-keyhole" : root || node.num_children ? "folder" : "file"));
  $("node-kind").title = node.protected ? "系统节点" : node.ephemeral ? "临时节点" : "持久节点";
  $("node-path").textContent = node.path;
  $("meta-version").textContent = node.version;
  $("meta-size").textContent = `${node.data_length} B`;
  $("meta-children").textContent = node.num_children;
  $("meta-modified").textContent = dateFormat.format(node.modified_at);
  setEncoding(node.encoding);
  setEditorValue(node.value);
  originalValue = node.value;
  updateEditor();
  renderNavigation();
}

async function selectNode(path) {
  selectedPath = path;
  const request = ++detailRequest;
  renderNavigation();
  showEmpty("正在读取…", "loader-circle");
  try {
    const next = await api("node", { parameters: { path } });
    if (request !== detailRequest) return;
    node = next;
    renderNode();
  } catch (error) {
    if (request === detailRequest) showEmpty("无法读取节点", "circle-alert", messageFor(error), true);
  }
}

function updateEditor() {
  $("editor-status").textContent = node?.protected ? "只读" : dirty() ? "有未保存修改" : "";
  updateControls();
}

function encodeBytes(bytes) {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 8192) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
  }
  return btoa(binary);
}

function convertEncoding(value, from, to) {
  if (from === to) return value;
  if (to === "base64") return encodeBytes(new TextEncoder().encode(value));
  const bytes = Uint8Array.from(atob(value.replace(/\s+/g, "")), (character) => character.charCodeAt(0));
  const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  if (/[\u0000-\u0008\u000b-\u000c\u000e-\u001f\u007f-\u009f]/.test(text)) throw new Error("invalid_data");
  return text;
}

function setDialogBusy(dialog, busy) {
  for (const button of dialog.querySelectorAll("button")) button.disabled = busy;
  for (const control of dialog.querySelectorAll("input, select, textarea")) control.disabled = busy;
  dialog.dataset.busy = busy;
}

async function changeConnection(disconnect) {
  if (connectionBusy || !allowDiscard()) return;
  const payload = {
    hosts: $("hosts").value,
    username: $("auth-options").open ? $("username").value : "",
    password: $("auth-options").open ? $("password").value : "",
  };
  connectionBusy = true;
  statusRequest += 1;
  updateControls();
  setMessage($("connection-error"), "");
  setDialogBusy(connectionDialog, true);
  $("connect-label").textContent = disconnect ? "正在断开…" : "正在连接…";
  try {
    const next = await api("connection", { method: disconnect ? "DELETE" : "POST", body: payload });
    connectionDialog.close();
    applyConnection(next);
  } catch (error) {
    setMessage($("connection-error"), messageFor(error));
  } finally {
    connectionBusy = false;
    $("connect-label").textContent = "连接";
    setDialogBusy(connectionDialog, false);
    updateControls();
    void pollStatus();
  }
}

async function changeNode(method, payload, dialog, errorElement, onSuccess) {
  const expectedId = connection.id;
  mutationBusy = true;
  updateControls();
  if (dialog) setDialogBusy(dialog, true);
  if (errorElement) setMessage(errorElement, "");
  try {
    const result = await api("node", { method, body: payload });
    if (expectedId !== connection.id) return;
    if (dialog) dialog.close();
    onSuccess(result);
  } catch (error) {
    if (expectedId === connection.id) {
      if (errorElement) setMessage(errorElement, messageFor(error));
      else notify(messageFor(error), true);
    }
  } finally {
    mutationBusy = false;
    if (dialog) setDialogBusy(dialog, false);
    updateControls();
  }
}

document.querySelectorAll("[data-close]").forEach((button) => {
  button.addEventListener("click", () => $(button.dataset.close).close());
});
for (const dialog of [connectionDialog, createDialog, importDialog, deleteDialog].filter(Boolean)) {
  dialog.addEventListener("cancel", (event) => {
    if (dialog.dataset.busy === "true") event.preventDefault();
  });
}
connectionDialog.addEventListener("close", () => { $("password").value = ""; });

$("open-connection").addEventListener("click", () => {
  if (connection.hosts) $("hosts").value = connection.hosts;
  $("auth-options").open = false;
  $("disconnect").hidden = !connection.id;
  setMessage($("connection-error"), "");
  connectionDialog.showModal();
});
$("connection-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void changeConnection(false);
});
$("disconnect").addEventListener("click", () => { void changeConnection(true); });

$("query").addEventListener("input", () => {
  $("clear-search").hidden = !($("query").value || searchState);
  updateControls();
});
$("search-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void runSearch();
});
$("leaves").addEventListener("change", () => { void runSearch(); });
$("clear-search").addEventListener("click", () => {
  clearSearch();
  $("query").focus();
});
$("refresh-tree").addEventListener("click", () => {
  if (searchState) {
    void runSearch();
    return;
  }
  for (const entry of entries.values()) {
    if (entry.open || entry.path === "/") void loadChildren(entry);
  }
});
navigation.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button || !connected() || mutationBusy || connectionBusy) return;
  const { action, path } = button.dataset;
  if (action === "more") void runSearch(true);
  if (action === "select" && (selectedPath !== path || node === null) && allowDiscard()) void selectNode(path);
  if (action === "toggle") {
    const entry = entries.get(path);
    entry.open = !entry.open;
    renderNavigation();
    if (entry.open && !entry.loading && (entry.children === null || entry.error)) void loadChildren(entry);
  }
});
$("locate-node").addEventListener("click", () => { void locatePath(node.path); });
$("copy-path").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(node.path);
    notify("已复制路径");
  } catch {
    notify("复制失败，请手动选择路径。", true);
  }
});
for (const id of ["refresh-node", "retry-detail"]) {
  $(id).addEventListener("click", () => {
    if (selectedPath !== null && allowDiscard()) void selectNode(selectedPath);
  });
}

valueInput.addEventListener("input", () => {
  const value = valueInput.value;
  draftValue = lineEndingSource ? restoreLineEndings(value, lineEndingSource) : value;
  updateEditor();
});
editor.addEventListener("change", (event) => {
  if (event.target.name !== "node-encoding") return;
  const next = event.target.value;
  let value;
  try {
    value = convertEncoding(draftValue, editingEncoding, next);
  } catch {
    setEncoding(editingEncoding);
    notify("当前数据无法转换为 UTF-8 文本。", true);
    return;
  }
  try {
    originalValue = convertEncoding(node.value, node.encoding, next);
  } catch {
    originalValue = null;
  }
  setEncoding(next);
  setEditorValue(value);
  updateEditor();
});
$("reset-node").addEventListener("click", renderNode);
editor.addEventListener("submit", (event) => {
  event.preventDefault();
  if ($("save-node").disabled) return;
  const payload = { path: node.path, version: node.version, value: draftValue, encoding: editingEncoding };
  void changeNode("PUT", payload, null, null, (saved) => {
    node = saved;
    renderNode();
    notify("节点数据已保存");
  });
});
window.addEventListener("beforeunload", (event) => {
  if (dirty()) {
    event.preventDefault();
    event.returnValue = true;
  }
});

$("open-create").addEventListener("click", () => {
  $("create-parent").textContent = node.path;
  $("create-form").reset();
  setMessage($("create-error"), "");
  createDialog.showModal();
});
$("create-form").addEventListener("submit", (event) => {
  event.preventDefault();
  if (mutationBusy || !connected() || !allowDiscard()) return;
  const payload = {
    parent: node.path,
    name: $("create-name").value,
    value: $("create-value").value,
    encoding: $("create-encoding").value,
  };
  void changeNode("POST", payload, createDialog, $("create-error"), ({ path }) => {
    void selectNode(path);
    void locatePath(path, { refreshParent: true });
    notify("子节点已创建");
  });
});

const importRequestLimit = 8 * 1024 * 1024;
const importNodeLimit = 4096;

async function readDirectory(root) {
  const source = { name: root.name, directories: [], files: [] };
  const pending = [{ handle: root, path: "" }];
  while (pending.length) {
    const { handle, path } = pending.pop();
    for await (const entry of handle.values()) {
      if (source.directories.length + source.files.length + 1 >= importNodeLimit) {
        throw new Error("import_too_many_nodes");
      }
      const relative = path ? `${path}/${entry.name}` : entry.name;
      if (entry.kind === "directory") {
        source.directories.push(relative);
        pending.push({ handle: entry, path: relative });
      } else {
        source.files.push({ path: relative, file: await entry.getFile() });
      }
    }
  }
  return source;
}

function readFolderFiles(files) {
  if (!files.length) return null;
  const source = { name: files[0].webkitRelativePath.split("/")[0], directories: [], files: [] };
  const prefix = `${source.name}/`;
  const directories = new Set();
  for (const file of files) {
    if (!source.name || !file.webkitRelativePath.startsWith(prefix)) throw new Error("invalid_import");
    const path = file.webkitRelativePath.slice(prefix.length);
    source.files.push({ path, file });
    for (let end = path.lastIndexOf("/"); end > 0; end = path.lastIndexOf("/", end - 1)) {
      const directory = path.slice(0, end);
      if (directories.has(directory)) break;
      directories.add(directory);
    }
    if (source.files.length + directories.size + 1 > importNodeLimit) throw new Error("import_too_many_nodes");
  }
  source.directories = [...directories];
  return source;
}

function renderImport() {
  const source = directoryImport?.source;
  const parent = $("import-parent").value;
  const summary = source
    ? `${source.name} · ${source.directories.length} 个子目录 · ${source.files.length} 个文件`
    : "";
  $("import-source").textContent = summary;
  $("import-target").hidden = !source;
  $("import-path").textContent = source ? `${parent.replace(/\/$/, "")}/${source.name}` : "";
  updateControls();
}

function chooseImportSource(state, source) {
  const input = $("import-parent");
  if (input.value === state.suggestedParent) {
    const anchor = state.anchor;
    state.suggestedParent = anchor.endsWith(`/${source.name}`)
      ? anchor.slice(0, anchor.lastIndexOf("/")) || "/"
      : anchor;
    input.value = state.suggestedParent;
  }
  state.source = source;
  setMessage($("import-error"), "");
  renderImport();
}

function importError(error) {
  const details = error.details ?? {};
  const path = details.path ? `（${details.path}）` : "";
  const partial = details.created || details.updated
    ? `已确认新建 ${details.created} 个节点，更新 ${details.updated} 个节点。`
    : "";
  return `${path}${messageFor(error)}${partial}`;
}

async function submitImport() {
  const state = directoryImport;
  if ($("import-submit").disabled || !allowDiscard()) return;
  const parent = $("import-parent").value;
  const { name, directories, files } = state.source;
  mutationBusy = true;
  setDialogBusy(importDialog, true);
  setMessage($("import-error"), "");
  $("import-caption").textContent = "正在导入…";
  updateControls();
  try {
    let size = 0;
    for (const { path, file } of files) {
      if (file.size >= 1024 * 1024) {
        const error = new Error("import_file_too_large");
        error.details = { path };
        throw error;
      }
      size += 4 * Math.ceil(file.size / 3);
      if (size >= importRequestLimit) throw new Error("import_request_too_large");
    }
    const payload = { parent, name, directories, files: [] };
    for (const { path, file } of files) {
      const bytes = new Uint8Array(await file.arrayBuffer());
      payload.files.push({ path, value: encodeBytes(bytes) });
    }
    if (!importDialog.open || state !== directoryImport || state.connectionId !== connection.id) return;
    const result = await api("import", { method: "POST", body: payload });
    if (state.connectionId !== connection.id) return;
    importDialog.close();
    entries.clear();
    const root = treeEntry({ name: "/", path: "/" });
    root.open = true;
    void selectNode(result.path);
    void locatePath(result.path, { expandTarget: true });
    notify(`导入完成。新建 ${result.created}，更新 ${result.updated}，无变化 ${result.unchanged}。`);
  } catch (error) {
    if (state === directoryImport) setMessage($("import-error"), importError(error));
  } finally {
    mutationBusy = false;
    setDialogBusy(importDialog, false);
    $("import-caption").textContent = "导入";
    updateControls();
  }
}

$("open-import")?.addEventListener("click", () => {
  directoryImport = { connectionId: connection.id, source: null, anchor: node.path, suggestedParent: node.path };
  $("import-form").reset();
  $("import-parent").value = node.path;
  setMessage($("import-error"), "");
  importDialog.showModal();
  renderImport();
});
importDialog?.addEventListener("close", () => {
  // A delayed close event must not clear a newly opened dialog.
  if (!importDialog.open) directoryImport = null;
});
$("import-parent")?.addEventListener("input", renderImport);
$("choose-folder")?.addEventListener("click", async () => {
  const state = directoryImport;
  try {
    if (!window.showDirectoryPicker) {
      $("import-files").click();
      return;
    }
    const handle = await window.showDirectoryPicker({ mode: "read" });
    if (state !== directoryImport || !importDialog.open) return;
    setDialogBusy(importDialog, true);
    updateControls();
    try {
      const source = await readDirectory(handle);
      if (state === directoryImport && importDialog.open) chooseImportSource(state, source);
    } finally {
      setDialogBusy(importDialog, false);
      updateControls();
    }
  } catch (error) {
    if (state !== directoryImport || !importDialog.open || error.name === "AbortError") return;
    if (error.name === "SecurityError" || error.name === "NotAllowedError") {
      $("import-files").click();
    } else {
      setMessage($("import-error"), importError(error));
    }
  }
});
$("import-files")?.addEventListener("change", (event) => {
  if (!directoryImport) return;
  try {
    const source = readFolderFiles(event.target.files);
    if (source) chooseImportSource(directoryImport, source);
  } catch (error) {
    setMessage($("import-error"), importError(error));
  } finally {
    event.target.value = "";
  }
});
$("import-form")?.addEventListener("submit", (event) => {
  event.preventDefault();
  void submitImport();
});

$("open-delete").addEventListener("click", () => {
  $("delete-path").textContent = node.path;
  setMessage($("delete-error"), "");
  deleteDialog.showModal();
});
$("delete-form").addEventListener("submit", (event) => {
  event.preventDefault();
  if (mutationBusy || !connected()) return;
  const payload = { path: node.path, version: node.version };
  void changeNode("DELETE", payload, deleteDialog, $("delete-error"), ({ path }) => {
    const parentPath = path.slice(0, path.lastIndexOf("/")) || "/";
    const parent = entries.get(parentPath);
    if (parent) {
      parent.children = parent.children?.filter((entry) => entry.path !== path) ?? null;
      void loadChildren(parent);
    }
    entries.delete(path);
    if (searchState) searchState.results = searchState.results.filter((result) => result.path !== path);
    void selectNode(parentPath);
    notify("节点已删除");
  });
});

updateControls();
void pollStatus();
setInterval(pollStatus, 4000);
