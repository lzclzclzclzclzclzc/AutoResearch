import { memo, useEffect, useMemo, useState } from 'react';
import { Background, Controls, Handle, MarkerType, MiniMap, Position, ReactFlow, applyNodeChanges } from '@xyflow/react';
import type { Node, NodeProps, NodeChange } from '@xyflow/react';
import ELK from 'elkjs/lib/elk.bundled.js';
import { create } from 'zustand';
import { ChevronDown, ChevronRight, Expand, Eye, FileText, FlaskConical, GitBranch, ListChecks, MessageSquare, Network, Play, Scale, Users } from 'lucide-react';
import type { Data } from '../api';
import { label } from '../api';
import { activeArbitration, arbitrationPhase } from '../arbitrations';

type ViewState = { selected: string | null; selectionKind: 'node' | 'edge'; collapsed: Record<string, boolean>; select: (id: string | null, kind?: 'node' | 'edge') => void; toggle: (id: string) => void };
export const useGraphView = create<ViewState>(set => ({ selected: null, selectionKind: 'node', collapsed: {}, select: (id, kind = 'node') => set({ selected: id, selectionKind: kind }), toggle: id => set(state => ({ collapsed: { ...state.collapsed, [id]: !state.collapsed[id] } })) }));
const elk = new ELK();
const names: Record<string, string> = { boss: 'Boss Group', exploration: 'Exploration', critic: 'Critic', implementation: 'Implementation', verify: 'Verify', organizer: 'Organizer', paper: 'Paper', god: 'God', watchdog: 'Watch Dog' };
const subtitles: Record<string, string> = { boss: '全局决策与共享记忆', exploration: '假设与实验设计', critic: '质疑与交叉审查', implementation: '实现、运行与监控', verify: '证据与结论验证', organizer: '本机资源安排', paper: '阶段实验报告', god:'组外自动仲裁与恢复', watchdog:'定时巡检、诊断与在线修复' };
const icons: Record<string, typeof Users> = { boss: Users, exploration: GitBranch, critic: ListChecks, implementation: Play, verify: FlaskConical, organizer: Network, paper: FileText, god: Scale, watchdog: Eye };
const RoleNode = memo(function RoleNode({ id, data, selected }: NodeProps) {
  const toggle = useGraphView(s => s.toggle);
  const role = String(data.role || ''); const member = role.endsWith('_member');
  const Icon = icons[role] || Users; const group = Boolean(data.member_count);
  return <div className={`role-node ${member ? 'member-node' : ''} ${selected ? 'is-selected' : ''} ${data.status === 'running' ? 'node-running' : ''}`}>
    <Handle type="target" position={Position.Top}/><Handle type="source" position={Position.Bottom}/>
    <div className="node-top"><Icon size={15}/><strong>{member ? String(data.label) : names[role] || String(data.label)}</strong>{Number(data.unread || 0)>0 && <span className="node-unread" title={`${data.unread} 条未读消息`} aria-label={`${data.unread} 条未读消息`}>{Number(data.unread)>99?'99+':String(data.unread)}</span>}{group && <button className="nodrag icon-button" title="展开或折叠成员" aria-label={`展开 ${names[role] || role} 成员`} onClick={e => { e.stopPropagation(); toggle(id); }}>{data.expanded ? <ChevronDown size={14}/> : <ChevronRight size={14}/>}</button>}</div>
    {!member && <div className="node-sub">{subtitles[role]}</div>}
    <div className="node-meta"><span className={`node-dot ${data.status}`}/><span>{String(data.arbitration_phase || label(String(data.status || 'idle')))}</span>{role === 'god' ? <span className="node-count">{Number(data.active_arbitrations || 0)} 项进行中</span> : group && <span className="node-count">{String(data.member_count)} 位成员</span>}</div>
    {data.line_id ? <div className="node-line">{String(data.line_id).replace('line-', '研究线 ')}</div> : null}
  </div>;
});
const nodeTypes = { role: RoleNode };

export default function ResearchGraph({ topology, lines, agents, arbitrations = [], watchdogEvents = [], researchId, executionStopped = false, onChoose }: { topology: Data; lines: Data[]; agents: Data[]; arbitrations?: Data[]; watchdogEvents?: Data[]; researchId: string; executionStopped?: boolean; onChoose?: () => void }) {
  const collapsed = useGraphView(s => s.collapsed); const select = useGraphView(s => s.select);
  const selected = useGraphView(s => s.selected); const kind = useGraphView(s => s.selectionKind);
  const [nodes, setNodes] = useState<Node[]>([]); const [layoutError, setLayoutError] = useState(false);
  const [scope, setScope] = useState('');
  const [readMarks,setReadMarks] = useState<Record<string,{seq:number;count:number}>>({});
  const raw: Data[] = topology.nodes || []; const rawEdges: Data[] = topology.edges || [];
  const lineIds = [...new Set(raw.map(n => n.data?.line_id).filter(Boolean))] as string[];
  const activeScope = scope || (lineIds.length > 3 ? lineIds[0] : 'all');
  const structure = JSON.stringify(raw.map(n => [n.id,n.parentId,n.data?.member_count]));
  const collapsedKey = JSON.stringify(collapsed);
  useEffect(()=>{select(null);setScope('');try{setReadMarks(JSON.parse(localStorage.getItem(`autoresearch:read:${researchId}`)||'{}'));}catch{setReadMarks({});}},[researchId,select]);
  const selectedSeq = raw.find(n=>n.id===selected)?.data?.latest_message_seq || 0;
  const selectedCount = raw.find(n=>n.id===selected)?.data?.message_count || 0;
  useEffect(()=>{if(!selected || kind!=='node' || !selectedSeq)return;setReadMarks(old=>{if((old[selected]?.seq||0)>=selectedSeq)return old;const next={...old,[selected]:{seq:selectedSeq,count:selectedCount}};localStorage.setItem(`autoresearch:read:${researchId}`,JSON.stringify(next));return next;});},[selected,selectedSeq,selectedCount,researchId,kind]);
  useEffect(() => {
    let alive = true;
    const visible = raw.filter(n => (!n.data?.line_id || activeScope === 'all' || n.data.line_id === activeScope) && (!n.parentId || collapsed[n.parentId]));
    const roots = visible.filter(n => !n.parentId);
    const height = (n: Data) => collapsed[n.id] ? 94 + 76 * Number(n.data?.member_count || 0) : 94;
    const makeNode = (n: Data, pos: Data): Node => ({ ...n, type: 'role', position: { x: pos.x || 0, y: pos.y || 0 }, style: { width: 190, height: n.parentId ? 66 : height(n) }, data: { ...n.data, expanded: !!collapsed[n.id] } }) as Node;
    const lanes = [...new Set(roots.map(n => n.data.line_id).filter(Boolean))] as string[];
    const graphs = lanes.map(lineId => {
      const peers = roots.filter(n => n.data.line_id === lineId);
      const chain = ['exploration','critic','implementation','verify'].map(role => peers.find(n => n.data.role === role)).filter(Boolean) as Data[];
      return elk.layout({ id: lineId, layoutOptions: { 'elk.algorithm': 'layered', 'elk.direction': 'DOWN', 'elk.layered.spacing.nodeNodeBetweenLayers': '28' }, children: chain.map(n => ({ id: n.id, width:190, height:height(n) })), edges: chain.slice(1).map((n,i) => ({ id:`layout-${n.id}`,sources:[chain[i].id],targets:[n.id] })) });
    });
    void Promise.all(graphs).then(results => {
      if (!alive) return;
      const saved = JSON.parse(localStorage.getItem(`autoresearch:layout-v2:${researchId}:${activeScope}`) || '{}');
      const globalHeight = Math.max(94,...roots.filter(n => !n.data.line_id).map(height));
      const topNodes = roots.map(n => {
        let pos: Data;
        if (!n.data.line_id) pos = {x:Math.max(0,['boss','organizer','paper','god','watchdog'].indexOf(n.id))*218,y:0};
        else { const lane = lanes.indexOf(n.data.line_id); const spot = results[lane]?.children?.find(p => p.id === n.id); pos = {x:lane*218 + (spot?.x || 0),y:globalHeight+48+(spot?.y || 0)}; }
        return makeNode(n,saved[n.id] || pos);
      });
      const children = visible.filter(n => n.parentId).map(n => { const siblings = raw.filter(s => s.parentId === n.parentId); return { ...makeNode(n,{ x: 8, y: 89 + siblings.findIndex(s => s.id === n.id) * 76 }), style: { width: 174, height: 66 }, draggable: false, zIndex: 2 }; });
      setNodes([...topNodes, ...children]); setLayoutError(false);
    }).catch(() => { if (alive) { setLayoutError(true); setNodes(visible.map((n,i) => makeNode(n,{ x: i%4*285, y: Math.floor(i/4)*180 }))); } });
    return () => { alive = false; };
    // Layout follows topology and expansion; execution updates never trigger layout.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [structure, collapsedKey, researchId, activeScope]);
  const stateNodes = useMemo(() => nodes.map(n => {
    const agent = agents.find(a => a.agent_id === n.id || a.id === n.id);
    const line = lines.find(l => l.line_id === n.data.line_id);
    const phase = String(line?.phase || ''); const role = String(n.data.role);
    const phaseRole: Record<string,string> = { exploring: 'exploration', exploration: 'exploration', criticizing: 'critic', critic: 'critic', implementing: 'implementation', implementation: 'implementation', running: 'implementation', verifying: 'verify', verify: 'verify', awaiting_boss: 'boss' };
    const current = raw.find(r=>r.id===n.id)?.data || {};
    const active = arbitrations.filter(activeArbitration);
    const scopeRequest = arbitrations.filter(request => activeArbitration(request) || ['waiting_children','waiting_dependencies'].includes(request.resolution_status)).find(request => request.scope_id === n.id || request.scope_id?.includes(`:${n.id}:`) ||
      (request.line_id === n.data.line_id && request.scope_kind === role));
    const watchdogState = watchdogEvents.some(event => event.status === 'diagnosing') ? 'running' : watchdogEvents.some(event => event.status === 'needs_attention') ? 'blocked' : 'idle';
    const resolvedStatus = n.id === 'watchdog' ? watchdogState : n.id === 'god' ? active.length ? 'running' : 'idle' : scopeRequest ? 'arbitrating' : agent?.status || (phaseRole[phase] === role ? 'running' : current.status || 'idle');
    const status = executionStopped && !['completed','failed','interrupted','cancelled'].includes(resolvedStatus) ? 'stopped' : resolvedStatus;
    return { ...n, selected: kind === 'node' && selected === n.id, data: { ...n.data, ...current, active_arbitrations:active.length,
      arbitration_phase:scopeRequest && !executionStopped ? arbitrationPhase(scopeRequest) : undefined,
      unread:Math.max(0,Number(current.message_count||0)-(readMarks[n.id]?.count||0)), expanded:!!collapsed[n.id], status } };
  }), [nodes, raw, lines, agents, arbitrations, watchdogEvents, selected, kind, collapsed,readMarks,executionStopped]);
  const edges = useMemo(() => rawEdges.filter(e => nodes.some(n => n.id === e.source) && nodes.some(n => n.id === e.target)).map(e => ({ ...e, id: e.id, source: e.source, target: e.target, type: 'smoothstep', label: selected === e.id ? e.label : e.data?.message_count ? String(e.data.message_count) : '', selected: kind === 'edge' && selected === e.id, markerEnd: { type: MarkerType.ArrowClosed, width: 13, height: 13 }, style: { stroke: selected === e.id ? '#1b6c80' : '#b7c4c7', strokeWidth: selected === e.id ? 2 : 1.2 }, labelStyle: { fontSize: 11, fill: '#5d5f60' }, labelBgStyle: { fill: '#fbfcfc' }, interactionWidth: 22 })), [rawEdges, nodes, selected, kind]);
  const changes = (changes: NodeChange[]) => setNodes(ns => applyNodeChanges(changes, ns));
  return <div className="graph-wrap"><div className="graph-caption"><span><span className="connection-dot"/>研究组织</span><select aria-label="查看研究线" value={activeScope} onChange={e => setScope(e.target.value)}><option value="all">全部研究线</option>{lineIds.map(l => <option key={l} value={l}>{l.replace('line-','研究线 ')}</option>)}</select></div>{layoutError && <div className="notice">自动布局不可用，已使用基础布局。</div>}<ReactFlow key={activeScope} nodes={stateNodes} edges={edges} nodeTypes={nodeTypes} onNodesChange={changes} onNodeClick={(_, n) => { select(n.id); onChoose?.(); }} onEdgeClick={(_, e) => { select(e.id, 'edge'); onChoose?.(); }} onPaneClick={() => select(null)} onNodeDragStop={(_, n) => { if (n.parentId) return; const storageKey=`autoresearch:layout-v2:${researchId}:${activeScope}`; const saved = JSON.parse(localStorage.getItem(storageKey) || '{}'); saved[n.id] = n.position; localStorage.setItem(storageKey, JSON.stringify(saved)); }} fitView fitViewOptions={{ padding: 0.12 }} minZoom={0.2} maxZoom={1.6} nodesConnectable={false} deleteKeyCode={null} proOptions={{ hideAttribution: true }}><Background color="#d7dfe0" gap={20} size={1}/><Controls showInteractive={false}/><MiniMap pannable zoomable nodeColor={n => n.data.status === 'running' ? '#1b6c80' : '#d8dfe0'} maskColor="rgba(244,247,247,.7)"/></ReactFlow><div className="graph-legend"><span><Users size={13}/>Group 可展开成员</span><span><MessageSquare size={13}/>连线关联实际交接</span><span className="text-button"><Expand size={13}/>拖动调整布局</span></div></div>;
}
