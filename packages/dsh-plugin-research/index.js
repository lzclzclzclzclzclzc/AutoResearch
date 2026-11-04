// Public raw ToolDefinition registration for the locked 0.2.1-rc.2 tools API.
// The Python gateway validates every canonical argument and source identity.
// This dependency-free bundle loads through an absolute official profile overlay.
export const name = 'autoresearch-research-tools'
export const inject = ['tools']

const delay = (ms, signal) => new Promise((resolve, reject) => {
  if (signal?.aborted) return reject(signal.reason || new Error('Aborted'))
  const abort = () => { clearTimeout(timer); reject(signal.reason || new Error('Aborted')) }
  const timer = setTimeout(() => { signal?.removeEventListener('abort', abort); resolve() }, ms)
  signal?.addEventListener('abort', abort, { once: true })
})

const object = (properties, required = []) => ({ type: 'object', properties, required, additionalProperties: false })
const string = { type: 'string' }
const strings = { type: 'array', items: string }

export function apply(ctx) {
  const endpoint = process.env.AUTORESEARCH_TOOL_GATEWAY
  const token = process.env.AUTORESEARCH_TOOL_CAPABILITY
  const role = process.env.AUTORESEARCH_ROLE
  if (!endpoint?.startsWith('http://127.0.0.1:') || !token || !role) throw new Error('Missing bound research tool capability')
  let streamCount = 0
  // SDK 0.2.1rc2 forwards committed Session events but no provider text deltas.
  // Yield the exact provider stream, but record text in bounded batches. Stable
  // Session tool/call and tool/result events carry complete tool arguments.
  ctx.on('llm/stream', async function* (options, next) {
    const streamId = ++streamCount
    let cursor = 0
    let pending = '', lastFlush = 0, started = false, recordingFailed = false
    const record = async chunk => {
      // Lost acknowledgements replay this exact cursor/body; the gateway deduplicates it.
      const body = JSON.stringify({ name: 'record_stream_event', arguments: { stream_id: streamId, cursor: ++cursor, chunk } })
      for (let attempt = 0; ; attempt++) {
        let response
        try {
          response = await fetch(endpoint, {
            method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
            body, signal: options.signal,
          })
        } catch (error) {
          if (options.signal?.aborted) throw error
          if (attempt >= 5) throw new Error('AUTORESEARCH_STREAM_RECORDER_UNAVAILABLE')
        }
        if (response?.ok) return
        if (response && ![408, 429, 500, 502, 503, 504].includes(response.status))
          throw new Error(response.status === 403 ? 'AUTORESEARCH_STREAM_RECORDER_STOPPED' : 'AUTORESEARCH_STREAM_RECORDER_REJECTED')
        if (attempt >= 5) throw new Error('AUTORESEARCH_STREAM_RECORDER_UNAVAILABLE')
        await delay(Math.min(100 * 2 ** attempt, 1000), options.signal)
      }
    }
    const flush = async () => {
      if (!pending) return
      await record({ type: 'text-delta', text: pending })
      pending = ''; lastFlush = Date.now(); started = true
    }
    try {
      for await (const chunk of next()) {
        if (!options.purpose && process.env.AUTORESEARCH_PRIVATE_TURN !== '1') {
          if (chunk.type === 'text-delta') {
            pending += chunk.text
            if (!started || pending.length >= 512 || Date.now() - lastFlush >= 250) await flush()
          } else {
            await flush()
            if (['usage', 'finish'].includes(chunk.type)) await record(chunk)
          }
        }
        yield chunk
      }
    } catch (error) {
      recordingFailed = true
      throw error
    } finally {
      if (!options.signal?.aborted && !recordingFailed) await flush()
    }
  })
  const definitions = role === 'god' ? [
    ['read_arbitration_context', 'Read only a section of this arbitration immutable snapshot. Returns offset, total length and SHA-256. Continue using next_offset; no other project or mutable source is accessible.', object({ section: string, offset: { type: 'integer', minimum: 0 } }, ['section'])],
  ] : [
    ['read_research_evidence', 'Read a registered, integrity-checked evidence artifact from this research line; paginate with offset.', object({ artifact_id: string, offset: { type: 'integer' } }, ['artifact_id'])],
    ['read_shared_memory', 'Read a Boss-managed shared memory node and its direct children. Empty node_id reads the root index.', object({ node_id: string })],
    ['send_research_message', 'Deliver a message to registered permitted recipients with durable per-member inboxes and a recoverable communication reply. It does not assign scientific tasks or create group votes.', object({ recipient_ids: strings, content_markdown: string }, ['recipient_ids', 'content_markdown'])],
  ]
  if (role === 'watchdog') definitions.push(
    ['read_watchdog_state', 'Read a section of this project state across groups, including tasks, executions, runs, discussions and prior repairs; paginate using next_offset.', object({ section: string, offset: { type: 'integer', minimum: 0 } }, ['section'])],
    ['apply_watchdog_action', 'Apply an auditable online repair after diagnosis. inject_prompt and resume_task target an existing task; restore_line_task targets a line whose parent task is missing; redeliver_handoff targets an outbox; reconcile_run targets a run. Stops, deadline, frozen artifacts and real process facts always prevail.', object({ action: { type: 'string', enum: ['inject_prompt', 'resume_task', 'restore_line_task', 'redeliver_handoff', 'reconcile_run'] }, target_id: string, content_markdown: string }, ['action', 'target_id'])],
  )
  if (process.env.AUTORESEARCH_HAS_INPUTS === '1' && ['exploration', 'critic', 'implementation', 'verify', 'boss'].includes(role)) definitions.push(
    ['list_research_inputs', 'List only the human-selected read-only input directory. path is relative; directory pages advance by next_offset. Every file inside the selected directory is readable, including hidden files and binary data. Paths outside it are unavailable.', object({ path: string, offset: { type: 'integer', minimum: 0 } })],
    ['read_research_input', 'Read any file relative to the human-selected input directory; binary pages use base64 and include a file path. Returns an immutable evidence artifact and source hash, with a response bounded to 64 KiB. Advance by next_offset; no filesystem writes or credentials.', object({ relative_path: string, offset: { type: 'integer', minimum: 0 } }, ['relative_path'])],
  )
  if (['exploration', 'critic', 'implementation', 'verify'].includes(role)) definitions.push(
    ['run_light_cpu', 'Run ordinary Python in a supervised child before the project deadline. No code capability restrictions or idle CPU core requirement. Imports (including numpy, pandas, os), functions, attributes, file access and subprocesses are allowed. Write/modify/delete files only inside workspace_dir (the assigned writable workspace). data_dir is the user-provided read-only data directory, or null; direct reads there are allowed. Libraries and subprocesses must follow the same workspace rule; do not access unrelated user files or credentials. Normal runtime/library loading is allowed. Read evidence[id]["path"] using the appropriate library. Save any result files inside workspace_dir; they are registered automatically. Setting result is optional; JSON is only a best-effort preview. Legacy evidence[id]["data"] is loaded on demand. Verify must reference existing artifacts. Memory and stop controls still apply.', object({ code: string, evidence_ids: strings }, ['code'])],
  )
  const allowed = new Set([...(role === 'god' ? [] : ['web_search', 'web_fetch', 'todo_write']), ...definitions.map(([name]) => name)])
  // Monotonic guard also denies new tools accidentally added to future profiles.
  if (role !== 'watchdog')
    ctx.tools.guard(execution => allowed.has(execution.name) ? undefined : 'This tool is outside the registered research role policy')
  for (const [name, description, parameters] of definitions) {
    ctx.tools.register({
      name, description, parameters,
      output: { schema: { type: 'object', additionalProperties: true }, render: (_args, value) => [{ type: 'text', text: JSON.stringify(value) }] },
      async execute(args, exec) {
        const response = await fetch(endpoint, {
          method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
          body: JSON.stringify({ name, arguments: args, call_id: exec.callId }), signal: exec.signal,
        })
        const value = await response.json()
        if (!response.ok) throw new Error(value.message || 'Research tool request rejected')
        return value
      },
    })
  }
}
