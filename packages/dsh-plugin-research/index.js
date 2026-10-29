// Public raw ToolDefinition registration for the locked 0.2.1-rc.2 tools API.
// The Python gateway validates every canonical argument and source identity.
// This dependency-free bundle loads through an absolute official profile overlay.
export const name = 'autoresearch-research-tools'
export const inject = ['tools']

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
    let pending = '', lastFlush = 0, started = false
    const record = async chunk => {
      const response = await fetch(endpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ name: 'record_stream_event', arguments: { stream_id: streamId, cursor: ++cursor, chunk } }), signal: options.signal,
      })
      if (!response.ok) throw new Error('Research stream recorder is unavailable or stopped')
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
    } finally {
      if (!options.signal?.aborted) await flush()
    }
  })
  const definitions = [
    ['read_research_evidence', 'Read a registered, integrity-checked evidence artifact from this research line; paginate with offset.', object({ artifact_id: string, offset: { type: 'integer' } }, ['artifact_id'])],
    ['read_shared_memory', 'Read a Boss-managed shared memory node and its direct children. Empty node_id reads the root index.', object({ node_id: string })],
    ['send_research_message', 'Deliver a message to registered permitted recipients with durable per-member inboxes and a recoverable communication reply. It does not assign scientific tasks or create group votes.', object({ recipient_ids: strings, content_markdown: string }, ['recipient_ids', 'content_markdown'])],
  ]
  if (process.env.AUTORESEARCH_HAS_INPUTS === '1' && ['exploration', 'critic', 'implementation', 'verify', 'boss'].includes(role)) definitions.push(
    ['list_research_inputs', 'List only the human-selected read-only input directory. path is relative; directory pages advance by next_offset. Protected files and paths outside that directory are unavailable.', object({ path: string, offset: { type: 'integer', minimum: 0 } })],
    ['read_research_input', 'Read a UTF-8 file relative to the human-selected input directory. Returns an immutable evidence artifact and source hash, with a response bounded to 64 KiB. Advance by next_offset; no filesystem writes or credentials.', object({ relative_path: string, offset: { type: 'integer', minimum: 0 } }, ['relative_path'])],
  )
  if (['exploration', 'critic', 'implementation', 'verify'].includes(role)) definitions.push(
    ['run_light_cpu', 'Run numeric-only lightweight CPU analysis before the research deadline. Evidence is preloaded as evidence; Verify must reference existing artifacts. No filesystem, network, process, formal training, or shared memory writes.', object({ code: string, evidence_ids: strings }, ['code'])],
  )
  const allowed = new Set(['web_search', 'web_fetch', 'todo_write', ...definitions.map(([name]) => name)])
  // Monotonic guard also denies new tools accidentally added to future profiles.
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
