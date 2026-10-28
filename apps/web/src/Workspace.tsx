import { useEffect, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowDownToLine, ArrowLeft, ArrowUpRight, Clock3, FileText, History, LayoutGrid, MessageSquare, Pause, Play, Square, Workflow } from 'lucide-react';
import { api, duration, items, label, terminal, time, write } from './api';
import type { Data } from './api';
import { Empty, ErrorNotice, Status } from './App';
import ResearchGraph from './components/ResearchGraph';
import Chat, { Markdown } from './components/Chat';
import { projectHistory } from './history';

const eventTypes = ['research.started','research.paused','research.resumed','research.terminating','research.terminated','research.completed','research.blocked','line.phase_changed','agent.started','agent.completed','agent.failed','agent.cancelled','message.started','message.delta','message.completed','message.interrupted','message.delivered','handoff.created','handoff.accepted','handoff.consumed','handoff.cancelled','outbox.delivered','boss.tasks_materialized','discussion.round_started','vote.recorded','group.decision_created','tool.started','tool.completed','tool.failed','allocation.queued','allocation.granted','allocation.starting','allocation.released','experiment.started','experiment.metric','experiment.stopped','experiment.failed','verification.completed','boss.decision_created','memory.version_created','report.completed'];
export default function Workspace({ history = false }: { history?: boolean }) {
  const { id = '' } = useParams(); const client = useQueryClient();
  const [tab, setTab] = useState('organization'); const [mobilePane, setMobilePane] = useState('graph');
  const [connected, setConnected] = useState(false); const [error, setError] = useState<unknown>(); const [busy, setBusy] = useState('');
  const [exportUrl, setExportUrl] = useState(''); const [streamRevision, setStreamRevision] = useState(0);
  const [tick, setTick] = useState(Date.now()); const [replaySeq, setReplaySeq] = useState(0); const [playing, setPlaying] = useState(false);
  const seq = useRef(0); const timeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  const snapshot = useQuery({ queryKey: ['snapshot',id], queryFn: () => api(`/api/research/${id}/snapshot`), refetchInterval: history ? false : 4000 });
  const control = useQuery({ queryKey: ['control',id], queryFn: () => api(`/api/research/${id}/control-status`), refetchInterval: 1000, retry: false });
  const timeline = useQuery({ queryKey: ['timeline',id], queryFn: async () => { let result: Data[] = []; let after = 0; for (;;) { const page = await api(`/api/research/${id}/timeline?after=${after}&limit=2000`); const rows = items(page); result = result.concat(rows); if (rows.length < 2000) break; after = rows.at(-1)!.seq; } return result; }, enabled: history, staleTime: Infinity });
  useEffect(() => { const timer = setInterval(() => setTick(Date.now()),1000); return () => clearInterval(timer); }, []);
  useEffect(() => { seq.current = 0; setConnected(false); }, [id,history]);
  useEffect(() => {
    if (!snapshot.data || history) return;
    seq.current = Math.max(seq.current,snapshot.data.snapshot_seq || 0);
    const source = new EventSource(`/api/research/${id}/events?after=${seq.current}`);
    const receive = (event: MessageEvent) => {
      let item: Data; try { item = JSON.parse(event.data); } catch { return; }
      if (item.seq && item.seq <= seq.current) return;
      seq.current = Math.max(seq.current,item.seq || 0);
      if (!timeout.current) timeout.current = setTimeout(() => {
        for (const key of ['snapshot','conversations','edge-messages','agent-executions']) void client.invalidateQueries({queryKey:[key,id]});
        void client.invalidateQueries({ queryKey: ['messages'] }); timeout.current = null;
      },200);
    };
    eventTypes.forEach(type => source.addEventListener(type, receive as EventListener)); source.onmessage = receive;
    source.onopen = () => setConnected(true); source.onerror = () => setConnected(false);
    source.addEventListener('snapshot_required', () => { source.close(); void snapshot.refetch().then(result => { seq.current=result.data?.snapshot_seq || 0; setStreamRevision(s=>s+1); }); });
    return () => { source.close(); if (timeout.current) { clearTimeout(timeout.current); timeout.current = null; } };
    // Snapshot updates are driven by the existing subscription, which keeps its server cursor.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  },[id,!!snapshot.data,history,streamRevision]);
  useEffect(() => { if (history && timeline.data?.length) setReplaySeq(timeline.data.at(-1)!.seq); }, [history,timeline.data]);
  useEffect(() => { if (!playing) return; const timer = setInterval(() => setReplaySeq(s => { const next = timeline.data?.find(e => e.seq > s)?.seq; if (!next) { setPlaying(false); return s; } return next; }),600); return () => clearInterval(timer); }, [playing,timeline.data]);
  const data = snapshot.data || {}; const p = data.project || {}; const c = control.data || {};
  const controlStopped = Boolean(c.stop_id || (c.stopped && c.state !== 'unknown'));
  const stopped = controlStopped || terminal(p.status) || p.status === 'terminating';
  const status = controlStopped && !terminal(p.status) ? 'terminating' : p.status;
  const selectedTime = history ? timeline.data?.find(e => e.seq === replaySeq)?.occurred_at || p.created_at : undefined;
  const endAt = selectedTime || p.completed_at || p.terminated_at || (terminal(p.status) ? p.stop_requested_at : undefined);
  const asOf = endAt ? new Date(endAt).getTime() : tick;
  const remaining = p.deadline_at ? (new Date(p.deadline_at).getTime()-asOf)/1000 : p.max_runtime_seconds || 0;
  const elapsed = p.started_at ? Math.max(0,(asOf-new Date(p.started_at).getTime())/1000) : 0;
  const command = async (kind: string) => { setBusy(kind); setError(undefined); try {
    if (history && kind !== 'export') return;
    if (kind === 'terminate') await write(`/api/research/${id}/terminate`,{});
    else if (kind === 'export') { const r = await write(`/api/research/${id}/exports`,{kind:'all'}); const artifact = r.artifact_id || r.artifact?.artifact_id || r.artifact?.id; if (artifact) setExportUrl(`/api/artifacts/${encodeURIComponent(artifact)}?download=true`); }
    else await write(`/api/research/${id}/commands`,{kind});
    void client.invalidateQueries({queryKey:['snapshot',id]}); void client.invalidateQueries({queryKey:['control',id]});
  } catch (e) { setError(e); } finally { setBusy(''); } };
  const maxSeq = timeline.data?.at(-1)?.seq || data.snapshot_seq || 1;
  const replayEvents = history ? (timeline.data || []).filter(e => e.seq <= replaySeq) : data.recent_events || [];
  const evidenceAt = history ? projectHistory(data,timeline.data || [],replaySeq) : data;
  const replayLines: Data[] = evidenceAt.lines || [];
  const executionStopped = history ? evidenceAt.project?.status === 'terminated' : p.status === 'terminated' || Boolean(controlStopped && c.execution_stopped);
  return <div className="workspace-page"><header className="workspace-header"><div className="workspace-title"><Link to="/" className="icon-button" aria-label="返回全部研究"><ArrowLeft size={17}/></Link><div><h1>{p.title || p.topic || '研究工作台'}</h1><div className="workspace-sub"><Status value={history ? evidenceAt.project?.status : status}/><span className="mono">{id.slice(-12)}</span>{p.mode === 'demo' && <span className="demo-tag">演示模式 · 预设 Agent 回应</span>}</div></div></div><div className="workspace-actions"><button className="button" onClick={() => void command('export')} disabled={busy === 'export'}><ArrowDownToLine size={14}/><span>导出</span></button>{!history && !stopped && <button className="button" onClick={() => void command(p.status === 'paused' || p.status === 'pausing' ? 'resume' : 'pause')} disabled={!!busy}>{p.status === 'paused' ? <Play size={14}/> : <Pause size={14}/>}<span>{p.status === 'paused' || p.status === 'pausing' ? '恢复' : '暂停新增工作'}</span></button>}{!history && <button className="button stop-button" onClick={() => void command('terminate')} disabled={busy === 'terminate'}><Square size={13}/>立即终止<span className="stop-explanation">（含运行中的实验）</span></button>}</div></header>
    <div className="workspace-meta"><div><Clock3 size={14}/><span>已用 <b className="mono">{duration(elapsed)}</b></span><span>剩余 <b className="mono">{duration(remaining)}</b></span><span className="muted time-note">暂停仍计时</span></div><div className="connection-state"><span className={`connection-dot ${!connected && !history ? 'off' : ''}`}/>{history ? '历史回放 · 只读' : connected ? '实时观察已连接' : '观察连接已断开，后台可能仍在运行'}</div></div>
    <ErrorNotice error={error} retry={() => setError(undefined)}/><ErrorNotice error={snapshot.error} retry={() => void snapshot.refetch()}/>{exportUrl && <div className="notice">导出已准备好。<a href={exportUrl} download className="text-button">下载完整研究记录 <ArrowDownToLine size={13}/></a></div>}
    {control.isError && <div className="notice error">独立控制通道状态未确认。终止入口仍可重试；不能据此认定实验已经退出。</div>}
    {controlStopped && <div className="notice stop-progress" role="status"><strong>{(c.unfinished || []).length ? '正在终止 / 核验进程退出' : c.db_synced ? '执行已停止，停止记录已同步' : '执行已停止，记录待同步'}</strong><span>{c.stop_journal_persisted === false ? '停止记录未持久化' : label(c.stop_reason || c.reason)}</span>{(c.unfinished || []).length > 0 && <details><summary>查看尚未退出的任务（{c.unfinished.length}）</summary><pre>{JSON.stringify(c.unfinished,null,2)}</pre></details>}</div>}
    {!history && p.status === 'blocked' && !controlStopped && <div className="notice"><span>{(p.blocked_reason ? label(p.blocked_reason) : undefined) || (typeof p.error === 'object' ? p.error.message : p.error) || '研究需要处理，请检查凭据、实验输入与运行环境。'}</span><Link to="/settings/providers" className="text-button">模型设置 <ArrowUpRight size={12}/></Link><button className="text-button" onClick={() => void command('rebind_credential')}>重绑凭据</button><button className="text-button" onClick={() => void command('retry')}>重试阻塞任务</button></div>}
    {history && <div className="replay-bar"><button className="button" onClick={() => { if (replaySeq >= maxSeq) setReplaySeq(0); setPlaying(!playing); }}>{playing ? <Pause size={14}/> : <Play size={14}/>}回放</button><label className="sr-only" htmlFor="replay">回放事件序号</label><input id="replay" type="range" min="0" max={maxSeq} value={replaySeq} onChange={e => { setPlaying(false); setReplaySeq(Number(e.target.value)); }}/><span className="mono">{replaySeq} / {maxSeq}</span><Link to={`/research/${id}`} className="text-button">返回实时工作台</Link></div>}
    <div className="workspace-tabs"><div className="tabs">{[['organization','研究组织',Workflow],['evidence','实验与证据',LayoutGrid],['memory','共享记忆',FileText],['reports','阶段报告',FileText]].map(([key,title,Icon]) => { const I = Icon as typeof Workflow; return <button key={String(key)} className={tab === key ? 'active' : ''} onClick={() => setTab(String(key))}><I size={14}/>{String(title)}</button>; })}</div>{!history && <Link to={`/research/${id}/history`} className="history-link"><History size={14}/>历史回放</Link>}</div>
    <div className="mobile-pane-switch"><button className={mobilePane === 'graph' ? 'active' : ''} onClick={() => setMobilePane('graph')}><Workflow size={14}/>研究视图</button><button className={mobilePane === 'chat' ? 'active' : ''} onClick={() => setMobilePane('chat')}><MessageSquare size={14}/>交流记录</button></div>
    <div className={`workbench-grid show-${mobilePane}`}><div className="research-main">{tab === 'organization' ? <><ResearchGraph researchId={id} executionStopped={executionStopped} topology={evidenceAt.topology || {}} lines={replayLines} agents={evidenceAt.agents || []} onChoose={() => setMobilePane('chat')}/><div className="line-strip">{replayLines.map((l: Data) => <div key={l.line_id}><span>{l.title || l.line_id}</span><strong>{label(executionStopped ? 'stopped' : l.phase)}</strong><small>方向 {l.direction_epoch} · Verify {l.completed_verify_rounds || 0} / {l.k || p.config_snapshot?.team?.k || 3}</small></div>)}</div></> : tab === 'evidence' ? <Evidence snapshot={evidenceAt}/> : tab === 'memory' ? <Memory researchId={id} asOf={history ? asOf : undefined}/> : <Reports reports={evidenceAt.reports || []}/>}
      <section className="activity-panel"><div className="section-heading"><h2>{history ? '回放事件' : '最近活动'}</h2><span className="muted small">事件 #{history ? replaySeq : data.snapshot_seq || 0}</span></div><div className="activity-list">{replayEvents.length ? [...replayEvents].reverse().slice(0,history ? 100 : 5).map((e: Data) => <div key={e.event_id}><time>{e.occurred_at ? new Date(e.occurred_at).toLocaleTimeString('zh-CN',{hour12:false}) : ''}</time><span>{eventLabel(e.type)}<small>{e.agent_id || e.line_id || e.payload?.message || ''}</small></span><span className="mono">#{e.seq}</span></div>) : <Empty title="等待研究活动">工作流状态与实验事件在持久化后显示。</Empty>}</div></section></div><Chat researchId={id} snapshot={evidenceAt} replaySeq={history ? replaySeq : undefined}/></div>
  </div>;
}
function eventLabel(type: string) { return ({ 'research.completed':'研究已完成','research.terminated':'研究已终止','research.terminating':'研究正在终止','boss.tasks_materialized':'Boss 决议已下发','handoff.accepted':'交接已送达','handoff.consumed':'已读取交接','handoff.cancelled':'交接已取消','outbox.delivered':'交付已确认','message.delivered':'消息已送达','research.created':'研究已创建','research.started':'研究已启动','research.blocked':'研究需要处理','agent.cancelled':'Agent 任务已中断','agent.started':'Agent 开始任务','agent.completed':'Agent 完成任务','agent.failed':'Agent 任务失败','line.phase_changed':'研究线进入新阶段','message.completed':'交流消息已记录','message.delta':'交流内容生成中','verification.completed':'验证完成','boss.decision_created':'Boss 决策已记录','report.completed':'阶段报告已生成','experiment.started':'正式实验已启动','experiment.stopped':'实验已退出','allocation.granted':'资源已分配','memory.version_created':'共享记忆已更新' } as Record<string,string>)[type] || type; }
function Evidence({ snapshot }: { snapshot: Data }) {
  const runs: Data[] = snapshot.runs || []; const submissions: Data[] = snapshot.submissions || [];
  return <div className="evidence-pane"><h2>实验运行 <span className="count">{runs.length}</span></h2>{runs.length ? runs.map(r => <details className="evidence-record" key={r.run_id || r.id}><summary><div><strong>{r.line_id || r.run_id || r.id}</strong><small>{time(r.started_at)} · {r.observation || '科学观察待记录'}</small></div><span>{label(r.status)}</span></summary><pre>{JSON.stringify(r,null,2)}</pre>{r.artifact_ids?.map((a: string) => <a key={a} href={`/api/artifacts/${encodeURIComponent(a)}`} target="_blank" rel="noreferrer">下载运行产物 <ArrowUpRight size={12}/></a>)}</details>) : <Empty title="尚无正式实验">方案冻结并取得资源授权后，原 Implementation 启动实验。</Empty>}<h2>提交包与验证 <span className="count">{submissions.length}</span></h2>{submissions.length ? submissions.map(s => <details className="evidence-record" key={s.submission_id || s.id}><summary><strong>Submission · {s.submission_id || s.id}</strong><span>v{s.version || 1}</span></summary><pre>{JSON.stringify(s,null,2)}</pre></details>) : <Empty title="尚无送验提交">计划、代码、运行、结果与 Verify 结论会绑定具体版本。</Empty>}</div>;
}
function Memory({ researchId, asOf }: { researchId: string; asOf?: number }) {
  const q = useQuery({ queryKey: ['memory',researchId], queryFn: () => api(`/api/research/${researchId}/memory`),refetchInterval:5000 });
  const candidates = asOf === undefined ? items(q.data) : (q.data?.history || []).filter((n: Data) => n.created_at && new Date(n.created_at).getTime() <= asOf).sort((a: Data,b: Data) => (a.memory_version || 0)-(b.memory_version || 0));
  const nodes: Data[] = [...new Map<string,Data>(candidates.map((n: Data): [string,Data] => [n.node_id || n.id,n])).values()]; const [chosen,setChosen] = useState(''); const current = nodes.find(n => (n.node_id || n.id) === chosen) || nodes[0];
  const tree:Data[]=[];const visited=new Set<string>();const ids=new Set(nodes.map(n=>n.node_id||n.id));
  const visit=(node:Data,depth:number)=>{const key=node.node_id||node.id;if(visited.has(key))return;visited.add(key);tree.push({...node,depth});nodes.filter(n=>n.parent_id===key).forEach(n=>visit(n,depth+1));};
  nodes.filter(n=>!n.parent_id||!ids.has(n.parent_id)).forEach(n=>visit(n,0));nodes.forEach(n=>visit(n,0));
  return <div className="memory-pane"><h2>共享记忆 <span className="muted small">仅 Boss 维护</span></h2><ErrorNotice error={q.error} retry={() => void q.refetch()}/>{nodes.length ? <div className="memory-layout"><div className="memory-tree">{tree.map(n => <button key={n.node_id || n.id} style={{paddingLeft: `${12 + n.depth * 8}px`}} className={(n.node_id || n.id) === (current?.node_id || current?.id) ? 'selected' : ''} onClick={() => setChosen(n.node_id || n.id)}>{n.title || n.path || n.node_id}</button>)}</div><article><h3>{current?.title || current?.path}</h3><span className="small muted">v{current?.version || 1} · {label(current?.status)}</span><Markdown content={current?.content_markdown || current?.content || ''}/><details><summary>来源与版本</summary><pre>{JSON.stringify(current,null,2)}</pre></details></article></div> : <Empty title="Boss 尚未写入共享记忆">记忆保留结论状态、适用条件、原始贡献者与证据来源。</Empty>}</div>;
}
function Reports({ reports }: { reports: Data[] }) { return <div className="reports-pane"><h2>阶段报告</h2>{reports.length ? reports.map(r => <article className="report" key={r.report_id || r.id}><div className="section-heading"><h3>{r.title || '当前进展报告'}</h3><span className="small muted">{time(r.created_at)}</span></div><Markdown content={r.content_markdown || r.content || ''}/>{(r.artifact_id || r.artifact_ids?.[0]) && <a className="button" href={`/api/artifacts/${encodeURIComponent(r.artifact_id || r.artifact_ids[0])}`} target="_blank" rel="noreferrer"><ArrowDownToLine size={14}/>下载报告</a>}<details><summary>冻结的证据快照</summary><pre>{JSON.stringify(r.snapshot || r.evidence_snapshot || r,null,2)}</pre></details></article>) : <Empty title="还没有阶段报告">Paper 按 Boss 下发的冻结证据快照写作，可与后续研究并行。</Empty>}</div>; }
