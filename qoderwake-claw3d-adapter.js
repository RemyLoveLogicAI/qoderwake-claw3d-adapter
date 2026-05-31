"use strict";

/**
 * Qoderwake Claw3D Adapter
 *
 * Bridges Claw3D's WebSocket gateway protocol to Qoderwake's CLI + SSE session API.
 *
 * Claw3D sends JSON-RPC-style method calls over WebSocket (e.g. chat.send, sessions.list).
 * This adapter translates them into `qoderwake session create/send/stream` CLI calls,
 * parses the SSE event stream, and maps Qoderwake worker events back as Claw3D chat events.
 *
 * Environment variables:
 *   QW_ADAPTER_PORT     WebSocket port              (default: 19889)
 *   QW_WAKER_ID         Qoderwake waker ID          (default: 3fb587205e49)
 *   QW_CLI_BIN          qoderwake CLI path          (default: qoderwake)
 *   QW_AGENT_NAME       Display name in Claw3D UI   (default: Qoderwake)
 *   QW_MODEL            Default model               (default: auto)
 */

const http = require("http");
const { WebSocketServer } = require("ws");
const { spawn, execFile } = require("child_process");
const crypto = require("crypto");

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const ADAPTER_PORT = parseInt(process.env.QW_ADAPTER_PORT || "19889", 10);
const DEFAULT_WAKER_ID = process.env.QW_WAKER_ID || "3fb587205e49";
const QW_CLI = process.env.QW_CLI_BIN || "qoderwake";
const AGENT_NAME = process.env.QW_AGENT_NAME || "Qoderwake";
const DEFAULT_MODEL = process.env.QW_MODEL || "auto";
const MAX_TOOL_ROUNDS = 8;
const HEARTBEAT_INTERVAL_MS = 25000; // Slightly under Claw3D's 30s tickIntervalMs

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------

/** @type {Map<string, {sessionId: string, wakerId: string, model: string, title: string, createdAt: number}>} */
const clawSessionMap = new Map(); // claw3d sessionKey -> qw session info

/** @type {Map<string, {runId: string, sessionKey: string, abort: () => void}>} */
const activeRuns = new Map();

/** @type {Set<(frame: object) => void>} */
const activeSendEventFns = new Set();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function randomId() {
  return crypto.randomBytes(8).toString("hex");
}

function redactSecrets(value) {
  if (typeof value !== "string" || !value) return value;
  return value
    .replace(/Bearer\s+[A-Za-z0-9._~+/=-]+/gi, "Bearer [REDACTED]")
    .replace(/\b[a-zA-Z0-9]{32,}\b/g, "[REDACTED]");
}

function sanitizeErrorMessage(error) {
  if (!error) return "Unknown error";
  if (typeof error === "string") return redactSecrets(error);
  return redactSecrets(error.message || String(error));
}

function resOk(id, payload) {
  return { type: "res", id, ok: true, payload: payload ?? {} };
}

function resErr(id, code, message) {
  return { type: "res", id, ok: false, error: { code, message } };
}

// ---------------------------------------------------------------------------
// Qoderwake CLI wrappers
// ---------------------------------------------------------------------------

function qwCli(args, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      proc.kill("SIGTERM");
      reject(new Error(`qoderwake CLI timeout after ${timeoutMs}ms: ${args.join(" ")}`));
    }, timeoutMs);

    const proc = execFile(QW_CLI, args, { maxBuffer: 50 * 1024 * 1024 }, (err, stdout, stderr) => {
      clearTimeout(timer);
      if (err) {
        const errMsg = stderr ? stderr.trim() : err.message;
        return reject(new Error(`qoderwake ${args[0]} failed: ${errMsg}`));
      }
      resolve({ stdout: stdout.trim(), stderr: (stderr || "").trim() });
    });
  });
}

async function qwSessionCreate(wakerId, message, title, cwd) {
  const args = [
    "session", "create",
    "--waker-id", wakerId,
    "--message", message,
    "--title", title,
    "--format", "json",
  ];
  if (cwd) args.push("--cwd", cwd);
  const { stdout } = await qwCli(args);
  return JSON.parse(stdout);
}

async function qwSessionSend(sessionId, message) {
  // Short timeout — we only need to confirm the message was queued, not wait for the response.
  // The response is consumed separately by qwSessionStream.
  const { stdout } = await qwCli([
    "session", "send",
    "--session-id", sessionId,
    "--message", message,
    "--format", "json",
  ], 5000);
  return JSON.parse(stdout);
}

async function qwSessionList() {
  const { stdout } = await qwCli(["session", "list", "--format", "json"]);
  return JSON.parse(stdout);
}

/**
 * Stream SSE events from a Qoderwake session.
 * Uses a push-based queue with a pending promise for reliable consumption.
 */
function qwSessionStream(sessionId, fromSeq) {
  const args = ["session", "stream", "--session-id", sessionId];
  if (fromSeq !== undefined) args.push("--from-sequence-num", String(fromSeq));

  const proc = spawn(QW_CLI, args, { stdio: ["ignore", "pipe", "pipe"] });
  let buffer = "";
  let aborted = false;
  let ended = false;

  // Push-based queue: items pushed, consumer awaits via pending promise
  const queue = [];
  let resolvePending = null;
  let pendingPromise = null;

  function push(item) {
    if (resolvePending) {
      const resolve = resolvePending;
      resolvePending = null;
      pendingPromise = null;
      resolve(item);
    } else {
      queue.push(item);
    }
  }

  function pull() {
    if (queue.length > 0) return Promise.resolve(queue.shift());
    if (ended) return Promise.resolve({ done: true });
    if (!pendingPromise) {
      pendingPromise = new Promise((resolve) => {
        resolvePending = resolve;
      });
    }
    return pendingPromise;
  }

  function parseBuffer() {
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith(":")) continue;
      if (trimmed.startsWith("data: ")) {
        try {
          const data = JSON.parse(trimmed.slice(6));
          push({ done: false, value: { type: "data", data } });
        } catch { /* skip malformed */ }
      }
    }
  }

  proc.stdout.on("data", (chunk) => {
    if (aborted) return;
    buffer += chunk.toString("utf8");
    parseBuffer();
  });

  proc.stdout.on("end", () => {
    if (aborted) return;
    ended = true;
    // Drain remaining buffer
    const lines = buffer.split("\n");
    buffer = "";
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.startsWith("data: ")) {
        try {
          const data = JSON.parse(trimmed.slice(6));
          push({ done: false, value: { type: "data", data } });
        } catch { /* skip */ }
      }
    }
    push({ done: true });
  });

  proc.stdout.on("error", (err) => {
    if (aborted) return;
    ended = true;
    push({ done: true, value: { type: "error", error: err } });
  });

  proc.stderr.on("data", (chunk) => {
    const msg = chunk.toString("utf8").trim();
    if (msg) console.log(`[qw-stream stderr ${sessionId}]:`, msg.slice(0, 200));
  });

  const abort = () => {
    if (aborted) return;
    aborted = true;
    proc.kill("SIGTERM");
    ended = true;
    push({ done: true });
  };

  return {
    abort,
    async next() {
      return pull();
    },
  };
}

// ---------------------------------------------------------------------------
// SSE Event Parser — extracts text content from Qoderwake events
// ---------------------------------------------------------------------------

/**
 * Parse a Qoderwake SSE payload and extract text delta or completion.
 * Returns { textDelta?, finalText?, isDone?, eventType }
 *
 * Qoderwake emits two kinds of response events:
 *  1. stream_event — Anthropic-style streaming (message_start, content_block_start,
 *     content_block_delta with text_delta/thinking_delta, message_stop)
 *  2. assistant    — snapshot-style with message.content array of text/thinking parts
 */
function parseQwEvent(payload) {
  if (!payload || typeof payload !== "object") return { eventType: "unknown" };

  const eventType = payload?.type || payload?.event_type || "";

  // --- Anthropic-style streaming events (wrapped in stream_event) -----------
  if (eventType === "stream_event") {
    const streamEv = payload?.event;
    if (!streamEv || typeof streamEv !== "object") return { eventType };

    const evType = streamEv.type;

    // Text delta — incremental text from the assistant
    if (evType === "content_block_delta" && streamEv.delta?.type === "text_delta") {
      return { textDelta: streamEv.delta.text || "", eventType: "stream_event.text" };
    }

    // Thinking delta
    if (evType === "content_block_delta" && streamEv.delta?.type === "thinking_delta") {
      return { eventType: "stream_event.thinking", thinking: streamEv.delta.thinking || "" };
    }

    // Message stop — end of response
    if (evType === "message_stop") {
      return { isDone: true, eventType: "stream_event.stop" };
    }

    return { eventType: `stream_event.${evType}` };
  }

  // --- Snapshot-style assistant events ---------------------------------------
  if (eventType === "assistant") {
    const content = payload?.message?.content;
    if (Array.isArray(content)) {
      const textParts = content
        .filter((p) => p && p.type === "text" && typeof p.text === "string")
        .map((p) => p.text)
        .join("");
      const thinkingParts = content
        .filter((p) => p && p.type === "thinking" && typeof p.thinking === "string")
        .map((p) => p.thinking);

      if (textParts) return { textDelta: textParts, eventType: "assistant.text" };
      if (thinkingParts.length) return { eventType: "assistant.thinking", thinking: thinkingParts.join("") };
    }
    return { eventType: "assistant" };
  }

  // --- Worker status changes — completion detection -------------------------
  if (eventType === "worker.status.changed") {
    const status = payload?.session_status || payload?.worker_status || "";
    if (status === "idle" || status === "stopped") {
      return { isDone: true, eventType, status };
    }
    return { eventType, status };
  }

  // --- Worker state changes -------------------------------------------------
  if (eventType === "worker_state") {
    return { eventType, state: payload?.state || "" };
  }

  // --- Snapshot — can signal completion -------------------------------------
  if (eventType === "worker.state.snapshot") {
    const sessionStatus = payload?.session_status || "";
    if (sessionStatus === "idle") {
      return { isDone: true, eventType, sessionStatus };
    }
    return { eventType, sessionStatus };
  }

  // --- User message echo (ignore) -------------------------------------------
  if (eventType === "user") return { eventType: "user" };

  return { eventType };
}

// ---------------------------------------------------------------------------
// Agentic loop — runs a Qoderwake session and streams text back
// Handles tool-use rounds: continues streaming through message_stop events
// until the session actually goes idle (worker.status.changed → idle/stopped).
// ---------------------------------------------------------------------------

async function runQwSession({ sessionKey, sessionId, userMessage, emitDelta, abortCheck, sendEvent }) {
  const stream = qwSessionStream(sessionId);
  let fullText = "";
  let hasContent = false;
  let textBuffer = "";
  let messageStopCount = 0;     // Track message_stop events for tool-use rounds
  let lastActivityAt = Date.now(); // For idle timeout detection
  const IDLE_TIMEOUT_MS = 120000; // 2 min with no activity = session done

  try {
    while (true) {
      if (abortCheck && abortCheck()) {
        stream.abort();
        break;
      }

      // Idle timeout: if no activity for IDLE_TIMEOUT_MS, consider the session done
      if (Date.now() - lastActivityAt > IDLE_TIMEOUT_MS) {
        console.log(`[qw-adapter] Session ${sessionId} idle timeout after ${IDLE_TIMEOUT_MS}ms`);
        break;
      }

      const item = await stream.next();
      if (item.done) break;

      if (item.value?.type === "error") {
        console.error(`[qw-adapter] Stream error for ${sessionId}:`, item.value.error?.message);
        break;
      }

      if (item.value?.type !== "data") continue;

      lastActivityAt = Date.now();
      const data = item.value.data;
      const parsed = parseQwEvent(data?.payload);

      // Track thinking activity for observability
      if (parsed.thinking) {
        console.log(`[qw-adapter] Session ${sessionId} thinking: ${parsed.thinking.slice(0, 80)}...`);
      }

      // Tool use detection: message_stop without text means a tool round boundary
      if (parsed.isDone && parsed.eventType === "stream_event.stop") {
        messageStopCount++;
        // Don't break yet — the session may continue with tool results.
        // Only break if we already have content AND we see a worker status change to idle.
        // For now, flush any buffered text and continue listening.
        if (textBuffer.length > 0) {
          emitDelta(fullText);
          textBuffer = "";
        }
        continue;
      }

      if (parsed.textDelta) {
        fullText += parsed.textDelta;
        textBuffer += parsed.textDelta;
        hasContent = true;
        // Batch text updates to reduce frame rate
        if (textBuffer.length > 20) {
          emitDelta(fullText);
          textBuffer = "";
        }
      }

      // Worker status changed to idle/stopped = true completion
      if (parsed.isDone && (parsed.eventType === "worker.status.changed" || parsed.eventType === "worker.state.snapshot")) {
        if (hasContent) {
          if (textBuffer.length > 0) {
            emitDelta(fullText);
            textBuffer = "";
          }
        }
        break;
      }
    }
  } catch (err) {
    console.error(`[qw-adapter] Stream error:`, sanitizeErrorMessage(err));
    if (!hasContent) throw err;
  } finally {
    stream.abort();
  }

  return fullText;
}

// ---------------------------------------------------------------------------
// Session management
// ---------------------------------------------------------------------------

function makeSessionKey(wakerId) {
  return `qw:${wakerId}:${randomId().slice(0, 8)}`;
}

function getOrCreateQwSession(sessionKey, wakerId) {
  let session = clawSessionMap.get(sessionKey);
  if (!session) {
    session = clawSessionMap.get(`qw:${wakerId}:main`);
  }
  return session;
}

// ---------------------------------------------------------------------------
// Method handlers
// ---------------------------------------------------------------------------

async function handleMethod(method, params, id, sendEvent) {
  const p = params || {};

  switch (method) {
    // --- Agents -----------------------------------------------------------

    case "agents.list": {
      const wakerId = DEFAULT_WAKER_ID;
      return resOk(id, {
        defaultId: wakerId,
        mainKey: `qw:${wakerId}:main`,
        agents: [{
          id: wakerId,
          name: AGENT_NAME,
          workspace: process.cwd(),
          identity: { name: AGENT_NAME, emoji: "🔧" },
          role: "AI Coding Agent",
        }],
      });
    }

    case "agents.create": {
      const agentName = (typeof p.name === "string" && p.name.trim()) ? p.name.trim() : "Agent";
      const slug = agentName.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
      const newId = `${slug}-${randomId().slice(0, 6)}`;
      return resOk(id, { agentId: newId, name: agentName, workspace: process.cwd() });
    }

    case "agents.delete":
    case "agents.update":
      return resOk(id, { ok: true, removedBindings: 0 });

    case "agents.files.get":
      return resOk(id, { file: { missing: true } });

    case "agents.files.set":
      return resOk(id, {});

    // --- Config -----------------------------------------------------------

    case "config.get":
      return resOk(id, {
        config: { gateway: { reload: { mode: "hot" } } },
        hash: "qw-adapter",
        exists: true,
      });

    case "config.patch":
    case "config.set":
      return resOk(id, { hash: "qw-adapter" });

    // --- Sessions -----------------------------------------------------------

    case "sessions.list": {
      const sessions = [];
      // List active Claw3D sessions
      for (const [key, info] of clawSessionMap.entries()) {
        sessions.push({
          key,
          agentId: info.wakerId,
          updatedAt: info.createdAt,
          displayName: info.title || "Main",
          origin: { label: AGENT_NAME, provider: "qoderwake" },
          model: info.model || DEFAULT_MODEL,
          modelProvider: "qoderwake",
        });
      }
      // Add a default main session if none exist
      if (sessions.length === 0) {
        const mainKey = `qw:${DEFAULT_WAKER_ID}:main`;
        sessions.push({
          key: mainKey,
          agentId: DEFAULT_WAKER_ID,
          updatedAt: null,
          displayName: "Main",
          origin: { label: AGENT_NAME, provider: "qoderwake" },
          model: DEFAULT_MODEL,
          modelProvider: "qoderwake",
        });
      }
      return resOk(id, { sessions });
    }

    case "sessions.preview": {
      const keys = Array.isArray(p.keys) ? p.keys : [];
      const previews = keys.map((key) => {
        const info = clawSessionMap.get(key);
        if (!info) return { key, status: "empty", items: [] };
        return { key, status: "ok", items: [{ role: "system", text: `Session: ${info.title}`, timestamp: info.createdAt }] };
      });
      return resOk(id, { ts: Date.now(), previews });
    }

    case "sessions.patch": {
      const key = typeof p.key === "string" ? p.key : `qw:${DEFAULT_WAKER_ID}:main`;
      const info = clawSessionMap.get(key);
      if (info && p.model !== undefined) {
        info.model = typeof p.model === "string" ? p.model.trim() : p.model;
      }
      return resOk(id, {
        ok: true,
        key,
        entry: {},
        resolved: { model: (info?.model) || DEFAULT_MODEL, modelProvider: "qoderwake" },
      });
    }

    case "sessions.reset": {
      const key = typeof p.key === "string" ? p.key : `qw:${DEFAULT_WAKER_ID}:main`;
      clawSessionMap.delete(key);
      return resOk(id, { ok: true });
    }

    // --- Chat ---------------------------------------------------------------

    case "chat.send": {
      let sessionKey = typeof p.sessionKey === "string" ? p.sessionKey : `qw:${DEFAULT_WAKER_ID}:main`;
      const userMessage = typeof p.message === "string" ? p.message.trim() : String(p.message || "").trim();
      const runId = (typeof p.idempotencyKey === "string" && p.idempotencyKey) ? p.idempotencyKey : randomId();

      if (!userMessage) return resOk(id, { status: "no-op", runId });

      const wakerId = sessionKey.startsWith("qw:") ? sessionKey.split(":")[1] : DEFAULT_WAKER_ID;
      const model = p.model || DEFAULT_MODEL;

      // Check if we have an existing session, or need to create one
      let qwSession = clawSessionMap.get(sessionKey);

      let aborted = false;
      activeRuns.set(runId, {
        runId,
        sessionKey,
        abort() { aborted = true; },
      });

      setImmediate(async () => {
        let seqCounter = 0;
        const emitChat = (state, extra) => {
          sendEvent({ type: "event", event: "chat", seq: seqCounter++,
            payload: { runId, sessionKey, state, ...extra } });
        };

        const onTextDelta = (partial) => {
          if (!aborted) emitChat("delta", { message: { role: "assistant", content: partial } });
        };

        try {
          let sessionId;

          if (qwSession) {
            // Use existing session — send follow-up message
            await qwSessionSend(qwSession.sessionId, userMessage);
            sessionId = qwSession.sessionId;
          } else {
            // Create new session
            const title = `Claw3D-${randomId().slice(0, 6)}`;
            const result = await qwSessionCreate(wakerId, userMessage, title);
            sessionId = result.sessionId;
            qwSession = { sessionId, wakerId, model, title, createdAt: Date.now() };
            clawSessionMap.set(sessionKey, qwSession);
          }

          // Stream the response
          const finalText = await runQwSession({
            sessionKey, sessionId, userMessage,
            emitDelta: onTextDelta,
            abortCheck: () => aborted,
            sendEvent,
          });

          if (aborted) {
            emitChat("aborted", {});
          } else {
            emitChat("final", {
              stopReason: "end_turn",
              message: { role: "assistant", content: finalText || "(no response)" },
            });
            // Presence update
            sendEvent({
              type: "event", event: "presence", seq: seqCounter++,
              payload: {
                sessions: {
                  recent: [{ key: sessionKey, updatedAt: Date.now() }],
                  byAgent: [{ agentId: wakerId, recent: [{ key: sessionKey, updatedAt: Date.now() }] }],
                },
              },
            });
          }
        } catch (err) {
          if (!aborted) {
            // Clean up stale session — next attempt should create a fresh one
            if (qwSession && clawSessionMap.get(sessionKey) === qwSession) {
              clawSessionMap.delete(sessionKey);
            }
            emitChat("error", { errorMessage: sanitizeErrorMessage(err) || "Qoderwake error" });
          } else {
            emitChat("aborted", {});
          }
        } finally {
          activeRuns.delete(runId);
        }
      });

      return resOk(id, { status: "started", runId });
    }

    case "chat.abort": {
      const runId = typeof p.runId === "string" ? p.runId.trim() : "";
      const sessionKey = typeof p.sessionKey === "string" ? p.sessionKey.trim() : "";
      let aborted = 0;
      if (runId) {
        const handle = activeRuns.get(runId);
        if (handle) {
          handle.abort();
          activeRuns.delete(runId);
          aborted += 1;
        }
      } else if (sessionKey) {
        for (const [rid, handle] of activeRuns.entries()) {
          if (handle.sessionKey !== sessionKey) continue;
          handle.abort();
          activeRuns.delete(rid);
          aborted += 1;
        }
      }
      return resOk(id, { ok: true, aborted });
    }

    case "chat.history": {
      const histKey = typeof p.sessionKey === "string" ? p.sessionKey : `qw:${DEFAULT_WAKER_ID}:main`;
      return resOk(id, { sessionKey: histKey, messages: [] });
    }

    case "agent.wait": {
      const { runId, timeoutMs = 30000 } = p;
      const start = Date.now();
      while (activeRuns.has(runId) && Date.now() - start < timeoutMs) {
        await new Promise((r) => setTimeout(r, 100));
      }
      return resOk(id, { status: activeRuns.has(runId) ? "running" : "done" });
    }

    // --- Approvals ----------------------------------------------------------

    case "exec.approvals.get":
      return resOk(id, {
        path: "", exists: true, hash: "qw-approvals",
        file: { version: 1, defaults: { security: "full", ask: "off", autoAllowSkills: true }, agents: {} },
      });

    case "exec.approvals.set":
      return resOk(id, { hash: "qw-approvals" });

    case "exec.approval.resolve":
      return resOk(id, { ok: true });

    // --- Status & heartbeat -------------------------------------------------

    case "status": {
      const recent = [...clawSessionMap.entries()].map(([key, info]) => ({
        key, updatedAt: info.createdAt,
      }));
      return resOk(id, {
        sessions: {
          recent,
          byAgent: [{ agentId: DEFAULT_WAKER_ID, recent }],
        },
      });
    }

    case "wake":
      return resOk(id, { ok: true });

    // --- Skills & models ----------------------------------------------------

    case "skills.status":
      return resOk(id, { skills: [] });

    case "models.list":
      return resOk(id, {
        models: [
          { id: "auto", name: "Auto" },
          { id: "sonnet", name: "Claude Sonnet" },
          { id: "opus", name: "Claude Opus" },
          { id: "haiku", name: "Claude Haiku" },
        ],
      });

    case "tasks.list":
      return resOk(id, { tasks: [] });

    // --- Cron jobs (not supported — stub) ----------------------------------

    case "cron.list":
      return resOk(id, { jobs: [] });

    case "cron.add":
    case "cron.remove":
    case "cron.patch":
    case "cron.run":
      return resOk(id, { ok: true });

    default:
      console.warn(`[qw-adapter] Unhandled method: ${method}`);
      return resOk(id, {});
  }
}

// ---------------------------------------------------------------------------
// WebSocket server
// ---------------------------------------------------------------------------

function startAdapter() {
  const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Qoderwake Claw3D Adapter – OK\n");
  });

  const wss = new WebSocketServer({ server: httpServer });
  wss.on("error", (err) => {
    if (err.code !== "EADDRINUSE") console.error("[qw-adapter] Server error:", sanitizeErrorMessage(err));
  });

  wss.on("connection", (ws) => {
    let connected = false;
    let globalSeq = 0;
    let heartbeatTimer = null;

    const send = (frame) => {
      if (ws.readyState === ws.OPEN) {
        try { ws.send(JSON.stringify(frame)); }
        catch (e) { console.error("[qw-adapter] send error:", sanitizeErrorMessage(e)); }
      }
    };

    // Register this connection's send function for broadcasts
    const sendEventFn = (frame) => {
      if (frame.type === "event" && typeof frame.seq !== "number") frame.seq = globalSeq++;
      send(frame);
    };
    activeSendEventFns.add(sendEventFn);

    send({ type: "event", event: "connect.challenge", payload: { nonce: randomId() } });

    const clearHeartbeat = () => {
      if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
    };

    ws.on("message", async (raw) => {
      let frame;
      try { frame = JSON.parse(raw.toString("utf8")); } catch { return; }
      if (!frame || typeof frame !== "object" || frame.type !== "req") return;
      const { id, method, params } = frame;
      if (typeof id !== "string" || typeof method !== "string") return;

      if (method === "connect") {
        connected = true;
        const allAgents = [{ agentId: DEFAULT_WAKER_ID, name: AGENT_NAME, isDefault: true }];
        send({
          type: "res", id, ok: true,
          payload: {
            type: "hello-ok", protocol: 3,
            adapterType: "qoderwake",
            features: {
              methods: [
                "agents.list", "agents.create", "agents.delete", "agents.update",
                "sessions.list", "sessions.preview", "sessions.patch", "sessions.reset",
                "chat.send", "chat.abort", "chat.history", "agent.wait",
                "status", "config.get", "config.set", "config.patch",
                "agents.files.get", "agents.files.set",
                "exec.approvals.get", "exec.approvals.set", "exec.approval.resolve",
                "wake", "skills.status", "models.list",
                "tasks.list",
                "cron.list", "cron.add", "cron.remove", "cron.patch", "cron.run",
              ],
              events: ["chat", "presence", "heartbeat", "cron"],
            },
            snapshot: {
              health: { agents: allAgents, defaultAgentId: DEFAULT_WAKER_ID },
              sessionDefaults: { mainKey: `qw:${DEFAULT_WAKER_ID}:main` },
            },
            auth: { role: "operator", scopes: ["operator.admin", "operator.approvals"] },
            policy: { tickIntervalMs: 30000 },
          },
        });
        // Start heartbeat after successful connect
        heartbeatTimer = setInterval(() => {
          send({ type: "event", event: "heartbeat", payload: { ts: Date.now(), activeRuns: activeRuns.size } });
        }, HEARTBEAT_INTERVAL_MS);
        return;
      }

      if (!connected) { send(resErr(id, "not_connected", "Send connect first.")); return; }

      try {
        const response = await handleMethod(method, params, id, sendEventFn);
        send(response);
      } catch (err) {
        const message = sanitizeErrorMessage(err);
        console.error(`[qw-adapter] Error handling ${method}:`, message);
        send(resErr(id, "internal_error", message || "Internal error"));
      }
    });

    ws.on("close", () => {
      clearHeartbeat();
      activeSendEventFns.delete(sendEventFn);
    });
    ws.on("error", (err) => {
      console.error("[qw-adapter] WebSocket error:", sanitizeErrorMessage(err));
      clearHeartbeat();
      activeSendEventFns.delete(sendEventFn);
    });
  });

  httpServer.listen(ADAPTER_PORT, "0.0.0.0", () => {
    console.log(`\n[qw-adapter] Listening on ws://0.0.0.0:${ADAPTER_PORT}`);
    console.log(`[qw-adapter] Forwarding to Qoderwake CLI: ${QW_CLI}`);
    console.log(`[qw-adapter] Default waker ID: ${DEFAULT_WAKER_ID}`);
    console.log(`[qw-adapter] Agent name: ${AGENT_NAME}`);
    console.log(`\nLocal:  ws://127.0.0.1:${ADAPTER_PORT}\n`);
  });

  httpServer.on("error", (err) => {
    if (err.code === "EADDRINUSE") {
      console.error(`[qw-adapter] Port ${ADAPTER_PORT} in use. Set QW_ADAPTER_PORT to change it.`);
    } else {
      console.error("[qw-adapter] Server error:", sanitizeErrorMessage(err));
    }
    process.exit(1);
  });
}

startAdapter();
