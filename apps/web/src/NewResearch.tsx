import { useEffect, useState } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowRight, ArrowUpRight, ChevronDown, Clock3, Loader2, Network } from 'lucide-react';
import { api, items, projectId, write } from './api';
import type { Data } from './api';
import { ErrorNotice } from './App';

export const defaults: Data = { n: 2, m1: 2, m2: 2, k: 3, boss_members: 3, model_concurrency: 4, experiment_concurrency: 1, watchdog_interval_minutes: 10, max_runtime_seconds: 3600 };
export function TeamFields({ value, onChange, advanced = true }: { value: Data; onChange: (v: Data) => void; advanced?: boolean }) {
  const fields = [['n', '并行研究线', '每条线独立探索与验证'], ['m1', 'Exploration 成员 / 线', '提出假设与实验方案'], ['m2', 'Critic 成员 / 线', '交叉审查与实验分歧'], ['k', '强制汇报周期 k', '第 k 次 Verify 后直接交 Boss']] as const;
  const numberField = (key: string, title: string, hint: string, max?: number) => <div className="field" key={key}><label htmlFor={`team-${key}`}>{title}</label><input type="number" id={`team-${key}`} min="1" max={max} required value={value[key] ?? defaults[key]} onChange={e => onChange({ ...value, [key]: e.target.value === '' ? '' : Number(e.target.value) })}/><small>{hint}</small></div>;
  return <><div className="field-grid">{fields.map(([key,title,hint]) => numberField(key,title,hint))}</div><div className="field"><label htmlFor="runtime">研究总运行时间（分钟）</label><input type="number" id="runtime" min="1" required value={value.max_runtime_seconds / 60} onChange={e => onChange({ ...value, max_runtime_seconds: Number(e.target.value) * 60 })}/><small>从实际启动起计时，排队、暂停与重启间隔均计入，所有研究线共用。</small></div>{advanced && <details className="advanced"><summary>高级配置 <ChevronDown size={14}/></summary><div className="field-grid">{numberField('boss_members','Boss 成员','全局研究决策，默认 3 人')}{numberField('model_concurrency','模型请求并发','限制同时在途的模型请求')}{numberField('experiment_concurrency','正式实验并发','同时受本机可用资源限制')}{numberField('watchdog_interval_minutes','Watch Dog 巡检间隔（分钟）','默认 10 分钟；仅发现研究线停滞时才调用模型诊断')}</div></details>}</>;
}
export function TeamPreview({ team }: { team: Data }) {
  const count = Number(team.n) * (Number(team.m1) + Number(team.m2) + 2) + Number(team.boss_members) + 3;
  return <div className="team-preview"><div><Network size={16}/><strong>{count || 0} 个 Agent</strong><span>{team.n || 0} 条研究线</span></div><div className="mini-org"><span>Boss × {team.boss_members}</span><i/><div>{Array.from({ length: Math.min(Number(team.n) || 0, 4) }, (_, i) => <span key={i}>研究线 {String(i + 1).padStart(2,'0')}</span>)}{Number(team.n) > 4 && <span>+{Number(team.n)-4}</span>}</div><i/><div><span>Organizer</span><span>Paper</span><span>Watch Dog</span></div></div><p>每线 Exploration {team.m1} · Critic {team.m2}<br/>Implementation 1 · Verify 1</p><div className="preview-caption"><span>模型并发 {team.model_concurrency || 4}</span><span>实验并发 {team.experiment_concurrency || 1}</span><span>巡检每 {team.watchdog_interval_minutes ?? 10} 分钟</span></div></div>;
}
export default function NewResearch() {
  const location = useLocation(); const navigate = useNavigate(); const client = useQueryClient();
  const q = useQuery({ queryKey: ['team'], queryFn: () => api('/api/settings/team') });
  const history = useQuery({ queryKey: ['projects'], queryFn: () => api('/api/research') });
  const [title, setTitle] = useState('');
  const [selectedHistory, setSelectedHistory] = useState('');
  const [importing, setImporting] = useState(false);
  const [configured, setConfigured] = useState(false);
  const [providerId, setProviderId] = useState('deepseek-default');
  const [resourceId, setResourceId] = useState('local-default');
  const [topic, setTopic] = useState(location.state?.topic || '');
  const [description, setDescription] = useState(''); const [workspace, setWorkspace] = useState('');
  const [team, setTeam] = useState<Data>(defaults); const [mode, setMode] = useState('live');
  const [busy, setBusy] = useState(false); const [error, setError] = useState<unknown>();
  useEffect(() => { if (q.data && !configured) setTeam({ ...defaults, ...(q.data.team || q.data) }); }, [q.data, configured]);
  const importHistory = async (id: string) => {
    setSelectedHistory(id); if (!id) return;
    setImporting(true); setError(undefined);
    try {
      const saved = await api(`/api/research/${id}/configuration`);
      setTopic(saved.topic); setDescription(saved.description || ''); setWorkspace(saved.workspace_path || '');
      setTeam({ ...defaults, ...saved.team, max_runtime_seconds: saved.max_runtime_seconds,
        model_concurrency: saved.model_concurrency, experiment_concurrency: saved.experiment_concurrency });
      setMode(saved.mode); setProviderId(saved.provider_config_id); setResourceId(saved.resource_profile_id); setConfigured(true);
    } catch (err) { setError(err); } finally { setImporting(false); }
  };
  const submit = async (e: React.FormEvent) => { e.preventDefault(); setBusy(true); setError(undefined); try {
    const result = await write('/api/research', { title: title.trim(), topic: topic.trim(), description, workspace_path: workspace || undefined, team: { n: team.n, m1: team.m1, m2: team.m2, k: team.k, boss_members: team.boss_members, watchdog_interval_minutes: team.watchdog_interval_minutes }, max_runtime_seconds: team.max_runtime_seconds, model_concurrency: team.model_concurrency, experiment_concurrency: team.experiment_concurrency, provider_config_id: providerId, resource_profile_id: resourceId, mode });
    await client.invalidateQueries({ queryKey: ['projects'] }); navigate(`/research/${result.research_id || result.project?.research_id || result.id}`);
  } catch (err) { setError(err); } finally { setBusy(false); } };
  return <div className="page new-page"><Link to="/" className="back-link">← 全部研究</Link><div className="page-title"><div><h1>新建研究</h1><p>明确问题与边界，团队会从研究设计开始推进。</p></div></div><form onSubmit={e => void submit(e)} className="new-layout"><div><section className="form-section"><h2>研究配置</h2><div className="field"><label htmlFor="history-config">导入历史配置 <span className="field-hint">可选</span></label><select id="history-config" value={selectedHistory} disabled={importing || history.isPending} onChange={e => void importHistory(e.target.value)}><option value="">选择一项历史任务</option>{items(history.data).map(p => <option key={projectId(p)} value={projectId(p)}>{p.title || '未命名研究'}</option>)}</select><small>{importing ? '正在导入配置…' : '复用研究内容、目录、团队和运行参数；请为新任务填写名称。模型使用当前保存的凭据。'}</small></div><div className="field"><label htmlFor="task-title">任务名称</label><input id="task-title" autoFocus required maxLength={200} value={title} onChange={e => setTitle(e.target.value)} placeholder="给这次任务起一个便于查找的名字"/></div><div className="field"><label htmlFor="topic">研究内容 / Prompt</label><textarea id="topic" rows={5} required maxLength={10000} value={topic} onChange={e => setTopic(e.target.value)} placeholder="你希望研究什么问题？"/></div><div className="field"><label htmlFor="description">目标与约束 <span className="field-hint">可选</span></label><textarea id="description" rows={4} value={description} onChange={e => setDescription(e.target.value)} placeholder="研究目标、对照条件、数据来源、已有工作与预期产物…"/></div><div className="field"><label htmlFor="workspace">已有代码或数据目录 <span className="field-hint">可选</span></label><input id="workspace" value={workspace} onChange={e => setWorkspace(e.target.value)} placeholder="本机绝对路径"/><small>缺少必要数据时，团队会提出补充信息请求并显示阻塞原因。</small></div></section><section className="form-section"><h2>团队与运行时间</h2><TeamFields value={team} onChange={v => { setTeam(v); setConfigured(true); }}/></section><section className="form-section"><h2>执行方式</h2><label className={`mode-option ${mode === 'live' ? 'selected' : ''}`}><input type="radio" name="mode" value="live" checked={mode === 'live'} onChange={() => setMode('live')}/><span><strong>真实研究</strong><small>完整 DSH、真实模型请求与本机实验。需要可用凭据与环境。</small></span></label><label className={`mode-option ${mode === 'demo' ? 'selected' : ''}`}><input type="radio" name="mode" value="demo" checked={mode === 'demo'} onChange={() => setMode('demo')}/><span><strong>演示模式</strong><small>使用预设 Agent 回应与小型 CPU 实验，演示内容不作为研究结论。</small></span></label></section><ErrorNotice error={error || history.error}/><div className="form-footer"><Link to="/" className="button">取消</Link><button type="submit" className="button primary" disabled={busy || importing || !topic.trim() || !title.trim()}>{busy ? <Loader2 size={15} className="spin"/> : <ArrowRight size={15}/>}创建{mode === 'demo' ? '演示' : ''}研究</button></div></div><aside className="new-aside"><h2>团队预览</h2><TeamPreview team={team}/><div className="aside-note"><Clock3 size={17}/><p>总运行时间 <strong>{team.max_runtime_seconds / 60} 分钟</strong><br/>暂停仍计时。任何阶段均可立即终止，包括运行中的实验。</p></div><Link to="/settings/providers" className="text-button">配置模型凭据 <ArrowUpRight size={14}/></Link></aside></form></div>;
}
