import { useEffect, useMemo, useRef, useState } from 'react';
import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { useVirtualizer } from '@tanstack/react-virtual';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';
import { ArrowDown, ArrowUpRight, ChevronLeft, MessageSquare, Search } from 'lucide-react';
import { api, items, label, time } from '../api';
import type { Data } from '../api';
import { Empty, ErrorNotice } from '../App';
import { useGraphView } from './ResearchGraph';
import ToolCall, { toolSearchText } from './ToolCall';
import ArbitrationDetails from './ArbitrationDetails';
import DiscussionVote from './DiscussionVote';
import { discussionVote, nextStepLabel, voteSearchText } from '../votes';
import { deduplicateGodMessages, godMessageValidation, godValidationLabels } from '../arbitrations';

export function Markdown({ content }: { content: string }) {
  return <div className="markdown"><ReactMarkdown remarkPlugins={[remarkGfm, remarkMath]} rehypePlugins={[rehypeKatex]} components={{ a: ({ href, children }) => href && (/^https?:\/\//i.test(href) || /^\/api\/artifacts\/[a-zA-Z0-9_-]+/.test(href)) ? <a href={href} target="_blank" rel="noreferrer">{children}</a> : <span>{children}</span>, img: ({ alt }) => <span className="muted">[图片：{alt || '附件'}]</span> }}>{content}</ReactMarkdown></div>;
}

function MessageContent({ message }: { message: Data }) {
  const vote = discussionVote(message);
  if (vote) return <><DiscussionVote vote={vote} markdown={content => <Markdown content={content}/>}/>{message.received_after_stop && <p className="message-provenance">停止后收到，已保留记录，未执行交接。</p>}</>;
  const final = message.structured;
  if (message.kind === 'decision' && final?.next_step && final?.final_output) return <>
    <p className="message-provenance">{message.received_after_stop ? '停止后收到 God 定稿，未执行交接' : 'God 最终定稿'} · {nextStepLabel(final.next_step)}</p>
    <Markdown content={final.final_output.content_markdown || message.content_markdown || ''}/>
    {Object.keys(final.final_output).some(key => key !== 'content_markdown') && <details className="vote-output"><summary>查看完整交付内容</summary><pre>{JSON.stringify(Object.fromEntries(Object.entries(final.final_output).filter(([key]) => key !== 'content_markdown')), null, 2)}</pre></details>}
  </>;
  const status = godValidationLabels[message.validation_status];
  if (message.validation_status === 'rejected') return <details>
    <summary>{status} · 展开回复与原因</summary>
    {message.validation_diagnostic && <p className="notice">{message.validation_diagnostic}</p>}
    <Markdown content={message.content_markdown || ''}/>
  </details>;
  return <>{status && <span className="message-provenance">{status}</span>}<Markdown content={message.content_markdown || ''}/></>;
}
export default function Chat({ researchId, snapshot, replaySeq }: { researchId: string; snapshot: Data; replaySeq?: number }) {
  const selected = useGraphView(s => s.selected); const kind = useGraphView(s => s.selectionKind); const select = useGraphView(s => s.select);
  const [conversation, setConversation] = useState(''); const [search, setSearch] = useState(''); const [external, setExternal] = useState(true);
  const [tab, setTab] = useState('chat'); const [direction, setDirection] = useState('all');
  const [cycle, setCycle] = useState('all'); const [unseen, setUnseen] = useState(false); const [query,setQuery] = useState('');
  const [reference, setReference] = useState<Data | null>(null); const [referenceError, setReferenceError] = useState<Error | null>(null);
  const referenceRequest = useRef(0);
  const conversationJump = useRef<string | null>(null);
  const viewport = useRef<HTMLDivElement>(null); const atBottom = useRef(true); const prevNewest = useRef(''); const prevBodyLength=useRef(0);
  const isGroup = Boolean(snapshot.topology?.nodes?.find((n:Data)=>n.id===selected)?.data?.member_count);
  const conversations = useQuery({ queryKey: ['conversations',researchId,selected,kind,replaySeq], queryFn: () => {
    const filters = new URLSearchParams();
    if (selected && kind === 'node') filters.set(isGroup ? 'group_id' : 'agent_id', selected);
    if (replaySeq !== undefined) filters.set('at_seq', String(replaySeq));
    return api(`/api/research/${researchId}/conversations?${filters}`);
  } });
  const all = items(conversations.data);
  const relevant = replaySeq === undefined ? all : all.filter(c=>Date.parse(c.created_at)<=Date.parse(snapshot.recent_events?.at(-1)?.occurred_at || snapshot.project?.created_at));
  useEffect(() => { setConversation(conversationJump.current || ''); conversationJump.current = null; setSearch(''); setTab('chat'); setCycle('all'); setDirection('all'); }, [selected,kind,researchId]);
  useEffect(()=>{const timer=setTimeout(()=>setQuery(search),200);return()=>clearTimeout(timer);},[search]);
  const current = relevant.find(c => (c.conversation_id || c.id) === conversation) || relevant[0];
  const currentId = current?.conversation_id || current?.id;
  useEffect(() => { referenceRequest.current++; setReference(null); setReferenceError(null); }, [selected,kind,researchId,replaySeq,currentId]);
  const edgeMessages = useInfiniteQuery({ queryKey: ['edge-messages',researchId,selected,query,replaySeq], initialPageParam: undefined as string | undefined, queryFn: ({pageParam}) => api(`/api/research/${researchId}/edges/${encodeURIComponent(selected!)}/messages?limit=100&q=${encodeURIComponent(query)}${replaySeq!==undefined?`&at_seq=${replaySeq}`:''}${pageParam ? `&before=${pageParam}` : ''}`), getNextPageParam:last => last.next_cursor || undefined, enabled: kind === 'edge' && !!selected });
  const messages = useInfiniteQuery({ queryKey: ['messages',currentId,query,replaySeq], initialPageParam: undefined as string | undefined, queryFn: ({ pageParam }) => api(`/api/conversations/${encodeURIComponent(currentId)}/messages?limit=50&q=${encodeURIComponent(query)}${replaySeq!==undefined?`&at_seq=${replaySeq}`:''}${pageParam ? `&before=${encodeURIComponent(pageParam)}` : ''}`), getNextPageParam: last => last.next_cursor || undefined, enabled: !!currentId && kind !== 'edge' });
  const tools = useInfiniteQuery({ queryKey: ['tool-calls',researchId,currentId,query,replaySeq], initialPageParam: undefined as string | undefined, queryFn: ({pageParam}) => api(`/api/research/${researchId}/tool-calls?conversation_id=${encodeURIComponent(currentId)}&limit=50&q=${encodeURIComponent(query)}${replaySeq!==undefined?`&at_seq=${replaySeq}`:''}${pageParam ? `&before=${encodeURIComponent(pageParam)}` : ''}`), getNextPageParam: last => last.next_cursor || undefined, enabled: !!currentId && kind !== 'edge' });
  const executions = useQuery({ queryKey: ['agent-executions',researchId,selected], queryFn: () => api(`/api/research/${researchId}/agents/${encodeURIComponent(selected!)}/executions`), enabled: !!selected && kind === 'node' });
  const traceItems: Data[] = useMemo(() => selected ? [...new Map([...(snapshot.executions || []).filter((x: Data) => x.agent_id === selected || x.agent_id?.startsWith(selected + '/')), ...(replaySeq === undefined ? items(executions.data) : [])].map((x: Data) => [x.execution_id || x.id,x])).values()] : snapshot.executions || [], [selected,snapshot.executions,replaySeq,executions.data]);
  const raw = useMemo(() => kind === 'edge' ? edgeMessages.data?.pages.flatMap(p => items(p)) || [] : [...(messages.data?.pages.flatMap(p => items(p)) || []), ...(tools.data?.pages.flatMap(p => items(p)) || [])], [kind,edgeMessages.data,messages.data,tools.data]);
  const historyQuery = kind === 'edge' ? edgeMessages : messages;
  const cycles = [...new Set(raw.map(m => m.cycle_id).filter(Boolean))];
  const visible = useMemo(() => deduplicateGodMessages([...new Map(raw.map(m => [m.message_id,m])).values()])
    .map(message => godMessageValidation(message, snapshot.arbitrations || [], traceItems)).filter(m => {
    const sender = m.sender?.id || m.authored_by_agent_id || '';
    const within = (value:string)=>value===selected || value.startsWith(`${selected}/`);
    const inGroup = !selected || (within(sender) && (m.recipients || []).every((r:Data)=>within(r.id)));
    const side = selected?.split('->');
    return (!search || (m.kind === 'tool_call' ? toolSearchText(m) : voteSearchText(m)).toLowerCase().includes(search.toLowerCase())) &&
      (external || inGroup) && (cycle === 'all' || m.cycle_id === cycle) &&
      (direction === 'all' || !side || (direction === 'forward' ? sender.startsWith(side[0]) : sender.startsWith(side[1]))) &&
      (replaySeq === undefined || Number(m.event_seq || m.seq || 0) <= replaySeq);
  }).sort((a,b) => (a.event_seq || a.seq || 0) - (b.event_seq || b.seq || 0) || String(a.created_at).localeCompare(String(b.created_at))), [raw,search,selected,external,cycle,direction,replaySeq,snapshot.arbitrations,traceItems]);
  const virtual = useVirtualizer({ count: visible.length, getScrollElement: () => viewport.current, estimateSize: () => 160, overscan: 4, getItemKey: i => visible[i]?.message_id || i });
  async function showReference(messageId: string) {
    const request = ++referenceRequest.current;
    setReferenceError(null); setReference(null);
    const index = visible.findIndex(message => message.message_id === messageId);
    if (index >= 0) { atBottom.current = false; virtual.scrollToIndex(index, { align: 'start' }); return; }
    try {
      const message = await api(`/api/research/${researchId}/messages/${encodeURIComponent(messageId)}${replaySeq!==undefined?`?at_seq=${replaySeq}`:''}`);
      if (request === referenceRequest.current) setReference(message);
    } catch (error) { if (request === referenceRequest.current) setReferenceError(error as Error); }
  }
  const newest = visible.at(-1)?.message_id || '';
  const newestLength = (visible.at(-1)?.content_markdown?.length || 0) + (visible.at(-1)?.latest_event_seq || 0);
  useEffect(() => {
    if (newest && (newest !== prevNewest.current || newestLength!==prevBodyLength.current)) {
      if (atBottom.current) { requestAnimationFrame(() => virtual.scrollToIndex(Math.max(0,visible.length - 1), { align: 'end' })); } else setUnseen(true);
    }
    prevNewest.current = newest;
    prevBodyLength.current=newestLength;
  }, [newest,newestLength,visible.length,virtual]);
  const title = selected ? snapshot.topology?.nodes?.find((n: Data) => n.id === selected)?.data?.label || (kind === 'edge' ? '连线交接' : selected) : '团队交流';
  return <section className="chat-pane" id="team-chat"><div className="chat-title"><div><MessageSquare size={16}/><h2>{title}</h2></div>{selected && <button className="text-button" onClick={() => select(null)}><ChevronLeft size={12}/>全部</button>}</div><div className="tabs chat-tabs"><button className={tab === 'chat' ? 'active' : ''} onClick={() => setTab('chat')}>交流记录</button><button className={tab === 'trace' ? 'active' : ''} onClick={() => setTab('trace')}>执行轨迹</button>{selected === 'god' && <button className={tab === 'arbitration' ? 'active' : ''} onClick={() => setTab('arbitration')}>仲裁详情</button>}</div>
    {tab === 'chat' ? <><div className="chat-filters">{kind !== 'edge' && relevant.length > 0 && <select aria-label="选择会话" value={currentId || ''} onChange={e => setConversation(e.target.value)}>{relevant.map(c => <option key={c.conversation_id || c.id} value={c.conversation_id || c.id}>{c.summary || c.title || c.conversation_id}</option>)}</select>}<div className="search"><Search size={13}/><input aria-label="搜索完整会话" placeholder="搜索完整会话" value={search} onChange={e => setSearch(e.target.value)}/></div>{selected && isGroup && kind === 'node' && <label className="checkbox"><input type="checkbox" checked={external} onChange={e => setExternal(e.target.checked)}/>含对外交流</label>}{kind === 'edge' && <select aria-label="消息方向" value={direction} onChange={e => setDirection(e.target.value)}><option value="all">全部方向</option><option value="forward">正向</option><option value="reverse">反向</option></select>}{cycles.length > 1 && <select aria-label="研究周期" value={cycle} onChange={e => setCycle(e.target.value)}><option value="all">全部周期</option>{cycles.map(c => <option key={c} value={c}>{c}</option>)}</select>}</div><ErrorNotice error={conversations.error || messages.error || edgeMessages.error || tools.error} retry={() => { void conversations.refetch(); if (kind === 'edge') void edgeMessages.refetch(); else if (currentId) { void messages.refetch(); void tools.refetch(); } }}/>
      <ErrorNotice error={referenceError}/>
      {reference && <aside className="reference-preview" aria-label="引用原文"><div><strong>引用原文</strong><button className="text-button" onClick={() => setReference(null)}>关闭</button></div><p className="small muted">{reference.sender?.id} · {time(reference.created_at)}</p><MessageContent message={reference}/></aside>}
      {(historyQuery.hasNextPage || (kind !== 'edge' && tools.hasNextPage)) && <button className="load-more" onClick={() => { if (historyQuery.hasNextPage) void historyQuery.fetchNextPage(); if (kind !== 'edge' && tools.hasNextPage) void tools.fetchNextPage(); }} disabled={historyQuery.isFetchingNextPage || tools.isFetchingNextPage}>{historyQuery.isFetchingNextPage || tools.isFetchingNextPage ? '读取中…' : '加载更早的记录'}</button>}
      <div className="message-viewport" ref={viewport} onScroll={e => { const el = e.currentTarget; atBottom.current = el.scrollHeight-el.scrollTop-el.clientHeight < 80; if (atBottom.current) setUnseen(false); }}>
        {visible.length ? <div style={{ height: virtual.getTotalSize(), position: 'relative' }}>{virtual.getVirtualItems().map(v => { const m = visible[v.index]; const sender = m.sender?.id || m.authored_by_agent_id || 'system'; const previous = visible[v.index-1]; return <div key={m.message_id} ref={virtual.measureElement} data-index={v.index} style={{ position: 'absolute', left: 0, top: 0, width: '100%', transform: `translateY(${v.start}px)` }}><article className="message" id={`message-${m.message_id}`}>{m.discussion_round && m.discussion_round !== previous?.discussion_round ? <div className="round-divider">讨论第 {m.discussion_round} 轮</div> : null}<div className="message-meta"><span className="avatar">{sender.startsWith('god') ? 'G' : sender.includes('critic') ? 'C' : sender.includes('boss') ? 'B' : sender.includes('exploration') ? 'E' : sender.includes('verify') ? 'V' : sender.includes('implementation') ? 'I' : 'S'}</span><button className="message-author" onClick={() => select(sender.startsWith('god/') ? 'god' : sender)} title={sender}>{sender.startsWith('god/') ? `God · ${sender.slice(4,16)}` : sender.split('/').slice(-2).join(' / ')}</button><time>{m.created_at ? new Date(m.created_at).toLocaleTimeString('zh-CN',{hour12:false,hour:'2-digit',minute:'2-digit'}) : ''}</time></div>{m.reply_to_message_id && <a className="reply-reference" href={`#message-${m.reply_to_message_id}`} onClick={e => { e.preventDefault(); void showReference(m.reply_to_message_id); }}>↳ 引用消息 {m.reply_to_message_id.slice(-8)}</a>}{m.kind === 'tool_call' ? <ToolCall call={m}/> : <MessageContent message={m}/>} {m.decision_source === 'god' && <span className="message-provenance">God 仲裁来源{m.arbitration_id ? ` · ${m.arbitration_id}` : ''}</span>}{m.arbitration_id && selected !== 'god' && <button className="text-button message-provenance" onClick={() => { conversationJump.current = `${researchId}:god:${m.arbitration_id}`; select('god'); }}>查看独立仲裁会话 <ArrowUpRight size={12}/></button>}{['partial','interrupted'].includes(m.status) && <span className="small muted">生成已中断，保留已有内容</span>}{m.artifact_refs?.length > 0 && <div className="attachment-list">{m.artifact_refs.map((a: any) => <a key={typeof a === 'string' ? a : a.artifact_id} href={`/api/artifacts/${encodeURIComponent(typeof a === 'string' ? a : a.artifact_id)}`} target="_blank" rel="noreferrer">附件 <ArrowUpRight size={12}/></a>)}</div>}</article></div>; })}</div> : <Empty title={currentId && (messages.isPending || (kind !== 'edge' && tools.isPending)) ? '正在读取交流记录…' : search ? '没有匹配的记录' : '还没有交流记录'}>这里展示 Agent 实际发送的消息、工具调用与交接。</Empty>}
      </div>{unseen && <button className="new-message" onClick={() => { virtual.scrollToIndex(visible.length-1,{align:'end'}); atBottom.current = true; setUnseen(false); }}><ArrowDown size={13}/>有新消息</button>}<div className="chat-foot">{visible.length} 条已加载记录 <span>按原始记录展示</span></div></> : tab === 'arbitration' ? <ArbitrationDetails researchId={researchId} requests={snapshot.arbitrations || []} conversationId={currentId} replaySeq={replaySeq} choose={setConversation}/> : <div className="trace-scroll">{traceItems.length ? traceItems.map((x: Data) => <details className="trace" key={x.execution_id || x.id}><summary><span>{x.agent_id || x.id}</span><span>{label(x.status)}</span></summary><p className="small muted">{time(x.started_at)} · attempt {x.attempt || 1}</p><pre>{JSON.stringify(x,null,2)}</pre></details>) : <Empty title="尚无执行轨迹">选择成员可查看其任务、工具调用与实际模型使用记录。</Empty>}</div>}
  </section>;
}
