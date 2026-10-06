//! In-process IDE bridge.
//!
//! A WebSocket MCP server that lives inside THIS Zed process, so a coding
//! agent (Claude Code today) connects to this window directly — no shared
//! `state.json`, no separate server process, no multi-window collision. Each
//! Zed process picks its own free port and writes its own lockfile, so N windows
//! pair 1:1 with N agent sessions.
//!
//! Threading: gpui's `AsyncApp`/`ForegroundExecutor` are `!Send`, but the WS
//! server needs tokio (`Send`). So the tokio side does socket I/O only and
//! never touches app state; every editor read is marshalled to the gpui main
//! thread over a `Send` channel and answered via a oneshot (the acp.rs idiom).

use buffer_diff::BufferDiff;
use editor::Editor;
use futures_util::{SinkExt, StreamExt};
use gpui::{App, AppContext, AsyncApp, WindowHandle};
use language::{Buffer, Capability};
use multi_buffer::MultiBuffer;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::Message;
use workspace::MultiWorkspace;

/// A request from the tokio WS server to run on the gpui main thread.
enum EditorRequest {
    Snapshot,
    WorkspaceFolders,
    OpenDiff { path: String, new_content: String },
}

type RespTx = oneshot::Sender<Value>;
type ReqTx = async_channel::Sender<(EditorRequest, RespTx)>;
type Conns = Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<String>>>>;

static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

/// Start an IDE server bound to ONE window. Called once per Zed window so N
/// windows each get their own port + lockfile and pair 1:1 with an agent.
pub fn spawn_window_instance(
    window: WindowHandle<MultiWorkspace>,
    cx: &mut gpui::Context<MultiWorkspace>,
) {
    let (req_tx, req_rx) = async_channel::unbounded::<(EditorRequest, RespTx)>();
    let (sel_tx, sel_rx) = async_channel::unbounded::<Value>();

    // [gpui main thread] reader bound to THIS window: services editor requests
    // + pushes this window's live selection (not the global active window).
    let reader_window = window.clone();
    cx.spawn(async move |_mw, cx: &mut AsyncApp| {
        let mut last = String::new();
        loop {
            while let Ok((req, resp_tx)) = req_rx.try_recv() {
                let _ = resp_tx.send(run_request(req, &reader_window, cx));
            }
            if let Some(notif) = build_selection_changed(&reader_window, cx) {
                let encoded = serde_json::to_string(&notif).unwrap_or_default();
                if encoded != last {
                    last = encoded;
                    let _ = sel_tx.try_send(notif);
                }
            }
            cx.background_executor()
                .timer(std::time::Duration::from_millis(100))
                .await;
        }
    })
    .detach();

    // [tokio] WS server on its own single-threaded runtime (one per window).
    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("[ide] tokio runtime failed: {e}");
                return;
            }
        };
        rt.block_on(run(req_tx, sel_rx));
    });
}

async fn run(req_tx: ReqTx, sel_rx: async_channel::Receiver<Value>) {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[ide] bind failed: {e}");
            return;
        }
    };
    let port = match listener.local_addr() {
        Ok(a) => a.port(),
        Err(e) => {
            eprintln!("[ide] local_addr failed: {e}");
            return;
        }
    };
    eprintln!("[ide] ws://127.0.0.1:{port}/mcp (in-process)");

    let conns: Conns = Arc::new(Mutex::new(HashMap::new()));

    // Keep the lockfile fresh: real workspace folders (known only after a
    // project opens) and self-heal if it's deleted. Rewrites on change or when
    // the file goes missing.
    {
        let refresh_tx = req_tx.clone();
        tokio::spawn(async move {
            let mut last_folders: Option<Vec<String>> = None;
            loop {
                let folders = request_workspace_folders(&refresh_tx).await;
                if folders.is_empty() {
                    // No project open (welcome/transient window) — not an IDE
                    // target, so stay undiscoverable: never publish a lockfile.
                    std::fs::remove_file(lock_path(port)).ok();
                    last_folders = Some(folders);
                } else if last_folders.as_ref() != Some(&folders) || !lock_path(port).exists() {
                    last_folders = Some(folders.clone());
                    write_lock_file(port, &folders);
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    // Broadcast live selection pushes to every connected agent.
    {
        let conns = conns.clone();
        tokio::spawn(async move {
            while let Ok(notif) = sel_rx.recv().await {
                let frame = serde_json::to_string(&notif).unwrap_or_default();
                for tx in conns.lock().unwrap().values() {
                    let _ = tx.send(frame.clone());
                }
            }
        });
    }

    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let conns = conns.clone();
        let req_tx = req_tx.clone();
        tokio::spawn(handle_conn(stream, conns, req_tx));
    }
}

/// One agent connection: speak MCP JSON-RPC over WS text frames.
async fn handle_conn(stream: tokio::net::TcpStream, conns: Conns, req_tx: ReqTx) {
    let ws = match tokio_tungstenite::accept_hdr_async(stream, |req: &http::Request<()>, mut resp: http::Response<()>| {
        // Negotiate the `mcp` WebSocket subprotocol: Claude Code sends
        // Sec-WebSocket-Protocol: mcp and waits for the server to echo it
        // before it sends `initialize` — without the echo it hangs at
        // "Connecting…" (the JetBrains plugin reads this same header).
        let name = http::HeaderName::from_static("sec-websocket-protocol");
        if let Some(proto) = req.headers().get(&name) {
            resp.headers_mut().insert(name, proto.clone());
        }
        Ok::<_, http::Response<Option<String>>>(resp)
    })
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("[ide] handshake failed: {e}");
            return;
        }
    };

    let id = CONN_SEQ.fetch_add(1, Ordering::Relaxed);
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    conns.lock().unwrap().insert(id, out_tx.clone());
    eprintln!("[ide] client connected #{id}");

    let (mut write, mut read) = ws.split();

    // Single writer: sends every outgoing JSON frame (responses + pushes).
    tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
    });

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(rpc) = serde_json::from_str::<Value>(text.as_str()) {
                    let method = rpc.get("method").and_then(|m| m.as_str()).unwrap_or("");
                    let tool = rpc["params"]["name"].as_str().unwrap_or("");
                    eprintln!("[ide] -> method={method} tool={tool}");
                    if let Some(resp) = handle_mcp(&rpc, &req_tx).await {
                        let _ = out_tx.send(serde_json::to_string(&resp).unwrap_or_default());
                    }
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    conns.lock().unwrap().remove(&id);
    eprintln!("[ide] client disconnected #{id}");
}

/// MCP dispatch. `initialize`/`tools/list` are local; `tools/call` marshals to
/// the main thread via `req_tx` when it needs editor state.
async fn handle_mcp(req: &Value, req_tx: &ReqTx) -> Option<Value> {
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req["method"].as_str().unwrap_or("");
    match method {
        "initialize" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "ide", "version": "0.1.0" }
            }
        })),
        "notifications/initialized" | "ide_connected" => None,
        "tools/list" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": { "tools": [
                {
                    "name": "getEditorContext",
                    "description": "Get the Zed editor context: the active file, cursor position, and any selected text (read live, in-process).",
                    "inputSchema": { "type": "object", "properties": {} }
                },
                {
                    "name": "openDiff",
                    "description": "Show the proposed change to a file as a diff in Zed (the current file vs the proposed content) for review.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "old_file_path": { "type": "string", "description": "Absolute path of the current file." },
                            "new_file_contents": { "type": "string", "description": "The proposed new file contents." }
                        },
                        "required": ["old_file_path", "new_file_contents"]
                    }
                }
            ] }
        })),
        "tools/call" => {
            let name = req["params"]["name"].as_str().unwrap_or("");
            match name {
                "getEditorContext" => {
                    let snap = request_snapshot(req_tx).await;
                    Some(tool_text(id, editor_context_text(snap)))
                }
                "openDiff" => {
                    let path = req["params"]["arguments"]["old_file_path"].as_str().unwrap_or("").to_string();
                    let new_content = req["params"]["arguments"]["new_file_contents"].as_str().unwrap_or("").to_string();
                    let _ = request_open_diff(req_tx, path, new_content).await;
                    Some(tool_text(id, "Opened the diff in Zed for review."))
                }
                _ => Some(tool_error(id, format!("tool {name}: not available in M1 spike"))),
            }
        }
        _ => None,
    }
}

async fn request_open_diff(req_tx: &ReqTx, path: String, new_content: String) -> Value {
    let (resp_tx, resp_rx) = oneshot::channel::<Value>();
    if req_tx.send((EditorRequest::OpenDiff { path, new_content }, resp_tx)).await.is_err() {
        return json!({});
    }
    resp_rx.await.unwrap_or_else(|_| json!({}))
}

async fn request_snapshot(req_tx: &ReqTx) -> Value {
    let (resp_tx, resp_rx) = oneshot::channel::<Value>();
    if req_tx.send((EditorRequest::Snapshot, resp_tx)).await.is_err() {
        return json!({});
    }
    resp_rx.await.unwrap_or_else(|_| json!({}))
}

fn tool_text(id: Value, text: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": { "content": [{ "type": "text", "text": text.into() }], "isError": false }
    })
}

fn tool_error(id: Value, text: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": { "content": [{ "type": "text", "text": text.into() }], "isError": true }
    })
}

// -- main-thread (gpui) side: all app-state reads live here ----------------

fn run_request(req: EditorRequest, window: &WindowHandle<MultiWorkspace>, cx: &mut AsyncApp) -> Value {
    match req {
        EditorRequest::Snapshot => read_selection(window, cx),
        EditorRequest::WorkspaceFolders => workspace_folders(window, cx),
        EditorRequest::OpenDiff { path, new_content } => {
            open_diff_view(&path, new_content, window, cx)
        }
    }
}

/// Open a read-only diff view: the current file at `path` vs `new_content`
/// (Claude's proposed change). Shows the comparison in a dedicated editor tab;
/// does NOT modify the file on disk.
fn open_diff_view(
    path: &str,
    new_content: String,
    window: &WindowHandle<MultiWorkspace>,
    cx: &mut AsyncApp,
) -> Value {
    let old_content = std::fs::read_to_string(path).unwrap_or_default();
    let title = std::path::Path::new(path)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "Diff".to_string());

    // Entity creation and `add_item_to_active_pane` need (&mut Window, &mut App).
    // `WindowHandle::update` runs the closure on this window's main thread with a
    // `Context<MultiWorkspace>` that derefs to `App` — so `cx.new` (AppContext) and
    // the `&mut App` coercion both resolve. Shows old vs proposed content; the file
    // on disk is untouched and there is nothing to accept or reject.
    let opened = window.update(cx, |multi_workspace, window, cx| {
        let cx: &mut App = cx;
        let workspace = multi_workspace.workspace().clone();
        let project = workspace.read(cx).project().clone();

        // "new" side = proposed content; base = current file, so the diff hunks
        // show exactly what the proposal changes. The file itself is not written.
        let new_buffer = cx.new(|cx| Buffer::local(new_content, cx));
        let new_snapshot = new_buffer.read(cx).snapshot();
        let base_buffer = cx.new(|cx| {
            let mut b = Buffer::local(old_content, cx);
            b.set_capability(Capability::ReadOnly, cx);
            b
        });
        let diff_val = BufferDiff::new_with_base_text_buffer(&new_snapshot, base_buffer, cx);
        let diff = cx.new(|_| diff_val);
        let multibuffer =
            cx.new(|cx| MultiBuffer::singleton(new_buffer, cx).with_title(title.clone()));
        multibuffer.update(cx, |multibuffer, cx| multibuffer.add_diff(diff, cx));

        let editor = cx.new(|cx| {
            let mut editor = Editor::for_multibuffer(multibuffer, Some(project), window, cx);
            editor.set_breadcrumb_header(title.clone());
            editor
        });

        workspace.update(cx, |workspace, cx| {
            workspace.add_item_to_active_pane(Box::new(editor), None, true, window, cx);
        });
    });

    match opened {
        Ok(()) => json!({ "ok": true, "message": "opened diff" }),
        Err(_) => json!({ "ok": false, "message": "window unavailable" }),
    }
}

async fn request_workspace_folders(req_tx: &ReqTx) -> Vec<String> {
    let (resp_tx, resp_rx) = oneshot::channel::<Value>();
    if req_tx.send((EditorRequest::WorkspaceFolders, resp_tx)).await.is_err() {
        return Vec::new();
    }
    resp_rx
        .await
        .ok()
        .and_then(|v| serde_json::from_value::<Vec<String>>(v).ok())
        .unwrap_or_default()
}

fn workspace_folders(window: &WindowHandle<MultiWorkspace>, cx: &mut AsyncApp) -> Value {
    let folders = window
        .update(cx, |multi_workspace, _window, cx| {
            let workspace = multi_workspace.workspace().clone();
            let folders: Vec<String> = workspace
                .read(cx)
                .worktrees(cx)
                .map(|wt| wt.read(cx).abs_path().to_string_lossy().into_owned())
                .collect();
            Some(folders)
        })
        .ok()
        .flatten()
        .unwrap_or_default();
    json!(folders)
}

fn read_selection(window: &WindowHandle<MultiWorkspace>, cx: &mut AsyncApp) -> Value {
    let snap = crate::claude_snapshot_selection(window, cx);
    serde_json::to_value(&snap).unwrap_or_else(|_| json!({}))
}

fn editor_context_text(snap: Value) -> String {
    let Some(obj) = snap.as_object() else {
        return "No active file yet. Open a file in Zed.".to_string();
    };
    let file = obj.get("filePath").and_then(|v| v.as_str()).unwrap_or("");
    if file.is_empty() {
        return "No active file yet. Open a file in Zed.".to_string();
    }
    let start = obj.get("startLine").and_then(|v| v.as_u64()).unwrap_or(0);
    let end = obj.get("endLine").and_then(|v| v.as_u64()).unwrap_or(0);
    let text = obj.get("text").and_then(|v| v.as_str()).unwrap_or("");
    let mut out = format!("Zed active file: {file}\nLines: {start}-{end}");
    if !text.is_empty() {
        out.push_str(&format!("\nSelected text:\n{text}"));
    }
    out
}

fn build_selection_changed(window: &WindowHandle<MultiWorkspace>, cx: &mut AsyncApp) -> Option<Value> {
    let snap = read_selection(window, cx);
    let file = snap.get("filePath").and_then(|v| v.as_str()).unwrap_or("");
    if file.is_empty() {
        return Some(json!({
            "jsonrpc": "2.0",
            "method": "selection_changed",
            "params": {}
        }));
    }
    let start = snap.get("startLine").and_then(|v| v.as_u64()).unwrap_or(1);
    let end = snap.get("endLine").and_then(|v| v.as_u64()).unwrap_or(1);
    let text = snap.get("text").and_then(|v| v.as_str()).unwrap_or("");
    let mut params = json!({
        "selection": {
            "start": { "line": start.saturating_sub(1), "character": 0 },
            "end":   { "line": end.saturating_sub(1),   "character": 1 }
        },
        "filePath": file
    });
    if !text.is_empty() {
        params["text"] = json!(text);
    }
    Some(json!({
        "jsonrpc": "2.0",
        "method": "selection_changed",
        "params": params
    }))
}

// -- lockfile / misc --------------------------------------------------------

static AUTH_TOKEN: OnceLock<String> = OnceLock::new();

fn lock_path(port: u16) -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::PathBuf::from(home)
        .join(".claude")
        .join("ide")
        .join(format!("{port}.lock"))
}

/// Iterate every `*.lock` as `(path, pid)`, skipping malformed ones.
fn each_lock_pid(mut f: impl FnMut(std::path::PathBuf, u64)) {
    let Ok(home) = std::env::var("HOME") else {
        return;
    };
    let dir = std::path::PathBuf::from(home).join(".claude").join("ide");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("lock") {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Some(pid) = serde_json::from_str::<Value>(&content)
            .ok()
            .and_then(|v| v.get("pid").and_then(|p| p.as_u64()))
        else {
            continue; // malformed lock — leave it, don't guess
        };
        f(path, pid);
    }
}

/// Remove lockfiles left by DEAD processes (previous Zed sessions each used a
/// different dynamic port). Runs once at startup so `~/.claude/ide/` doesn't
/// accumulate stale entries forever.
pub fn cleanup_stale_locks() {
    let my_pid = std::process::id() as u64;
    each_lock_pid(|path, pid| {
        if pid != my_pid && !pid_alive(pid as u32) {
            std::fs::remove_file(&path).ok();
        }
    });
}

/// Remove all lockfiles THIS process wrote (pid == ours). Called on app quit so
/// a clean Dock quit doesn't leave phantom Zed entries in Claude's /ide.
pub fn remove_my_lockfiles() {
    let my_pid = std::process::id() as u64;
    each_lock_pid(|path, pid| {
        if pid == my_pid {
            std::fs::remove_file(&path).ok();
        }
    });
}

#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // kill(pid, 0) with sig 0: returns 0 → alive; -1 with ESRCH → dead; any
    // other errno (e.g. EPERM) → the process exists but we lack perms → alive.
    let r = unsafe { libc::kill(pid as libc::pid_t, 0) };
    r == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> bool {
    true // conservative off-unix: never treat a lock as stale
}

/// Write this process's IDE lockfile so Claude Code discovers us via the port.
/// `folders` are the currently-open workspace roots (real paths, not HOME).
fn write_lock_file(port: u16, folders: &[String]) {
    if let Some(dir) = lock_path(port).parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let lock = json!({
        "pid": std::process::id(),
        "workspaceFolders": folders,
        "ideName": "Zed",
        "transport": "ws",
        "runningInWindows": false,
        "authToken": AUTH_TOKEN.get_or_init(random_token)
    });
    std::fs::write(lock_path(port), serde_json::to_string_pretty(&lock).unwrap()).ok();
}

fn random_token() -> String {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom").expect("open urandom");
    let mut buf = vec![0u8; 32];
    f.read_exact(&mut buf).expect("read urandom");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}
