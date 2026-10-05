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

use futures_util::{SinkExt, StreamExt};
use gpui::AsyncApp;
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
}

type RespTx = oneshot::Sender<Value>;
type ReqTx = async_channel::Sender<(EditorRequest, RespTx)>;
type Conns = Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<String>>>>;

static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

/// Start the in-process WS server. Called once at app startup.
pub fn init(cx: &mut gpui::App) {
    let (req_tx, req_rx) = async_channel::unbounded::<(EditorRequest, RespTx)>();
    let (sel_tx, sel_rx) = async_channel::unbounded::<Value>();

    // [gpui main thread] owns AsyncApp: services editor requests + pushes the
    // live selection. Replaces the state.json poller.
    cx.spawn(async move |cx: &mut AsyncApp| {
        let mut last = String::new();
        loop {
            while let Ok((req, resp_tx)) = req_rx.try_recv() {
                let _ = resp_tx.send(run_request(req, cx));
            }
            if let Some(notif) = build_selection_changed(cx) {
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

    // [tokio] WS server — Send only; app state reached via the channels above.
    let handle = gpui_tokio::Tokio::handle(cx);
    handle.spawn(run(req_tx, sel_rx));
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
                if last_folders.as_ref() != Some(&folders) || !lock_path(port).exists() {
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
                _ => Some(tool_error(id, format!("tool {name}: not available in M1 spike"))),
            }
        }
        _ => None,
    }
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

fn run_request(req: EditorRequest, cx: &mut AsyncApp) -> Value {
    match req {
        EditorRequest::Snapshot => read_selection(cx),
        EditorRequest::WorkspaceFolders => workspace_folders(cx),
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

fn workspace_folders(cx: &mut AsyncApp) -> Value {
    let folders = cx
        .update(|cx| cx.active_window().and_then(|w| w.downcast::<MultiWorkspace>()))
        .and_then(|window| {
            window
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
        })
        .unwrap_or_default();
    json!(folders)
}

fn read_selection(cx: &mut AsyncApp) -> Value {
    let snap = cx
        .update(|cx| cx.active_window().and_then(|w| w.downcast::<MultiWorkspace>()))
        .and_then(|window| crate::claude_snapshot_selection(&window, cx));
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

fn build_selection_changed(cx: &mut AsyncApp) -> Option<Value> {
    let snap = read_selection(cx);
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
