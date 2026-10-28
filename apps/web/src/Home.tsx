import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowRight, ArrowUpRight, Check, ChevronRight, Clock3, FlaskConical, Network, Plus, Search, Square, KeyRound, Trash2, Undo2 } from 'lucide-react';
import { api, items, projectId, time, terminal, write } from './api';
import type { Data } from './api';
import { Empty, ErrorNotice, Status } from './App';

export default function Home() {
  const navigate = useNavigate();
  const client = useQueryClient();
  const [topic, setTopic] = useState('');
  const [filter, setFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [error, setError] = useState<unknown>();
  const [pendingDelete, setPendingDelete] = useState<Data>();
  const [changing, setChanging] = useState('');
  const [stops, setStops] = useState<Record<string,Data>>({});
  const projects = useQuery({ queryKey: ['projects', 'with-deleted'], queryFn: () => api('/api/research?include_deleted=true'), refetchInterval: 5000 });
  const providers = useQuery({ queryKey: ['providers'], queryFn: () => api('/api/settings/providers') });
  const records = items(projects.data);
  const all = records.filter(p => !p.deleted_at);
  const stopIds = Object.keys(stops);
  const stopProgress = useQuery({queryKey:['list-control',stopIds.join(',')],enabled:stopIds.length>0,queryFn:async()=>Object.fromEntries(await Promise.all(stopIds.map(async id=>[id,await api(`/api/research/${id}/control-status`).catch(()=>({...stops[id],unconfirmed:true}))]))),refetchInterval:1000});
  useEffect(()=>{if(stopProgress.data)setStops(old=>({...old,...stopProgress.data}));},[stopProgress.data]);
  const active = all.filter(p => !terminal(p.status));
  const ready = items(providers.data).some(p => p.configured || p.credential_status === 'configured' || p.has_credential);
  const visible = records.filter(p => (filter === 'deleted' ? !!p.deleted_at : !p.deleted_at && (filter === 'all' || (filter === 'active' ? !terminal(p.status) : terminal(p.status)))) && (p.title || p.topic || '').toLowerCase().includes(search.toLowerCase()));
  const changeVisibility = async (p: Data, deleted: boolean) => {
    const id = projectId(p); setChanging(id); setError(undefined);
    try {
      if (deleted) await api(`/api/research/${id}`, { method: 'DELETE' });
      else await write(`/api/research/${id}/restore`, {});
      setPendingDelete(undefined); await client.invalidateQueries({ queryKey: ['projects'] });
    } catch (e) { setError(e); } finally { setChanging(''); }
  };
  const recordActions = (p: Data) => {
    const id = projectId(p); const removable = ['completed', 'terminated', 'quarantined'].includes(p.status);
    return <div className="record-actions">{p.deleted_at ? <button className="icon-button" title="恢复记录" aria-label={`恢复 ${p.title}`} disabled={!!changing} onClick={() => void changeVisibility(p, false)}><Undo2 size={16}/></button> : <>{!terminal(p.status) ? <button className="icon-button danger-text" title="立即终止（含实验）" aria-label={`立即终止 ${p.title || p.topic}`} onClick={() => void stop(id)}><Square size={14}/></button> : <Link className="icon-button" aria-label="查看研究" to={`/research/${id}`}><ArrowUpRight size={16}/></Link>}<button className="icon-button danger-text" title={removable ? '删除记录' : '请先终止研究，再删除记录'} aria-label={`删除 ${p.title || p.topic}`} disabled={!removable || !!changing} onClick={() => setPendingDelete(p)}><Trash2 size={15}/></button></>}</div>;
  };
  const stop = async (id: string) => { try { const state=await write(`/api/research/${id}/terminate`, {});setStops(old=>({...old,[id]:state})); void client.invalidateQueries({ queryKey: ['projects'] }); } catch (e) { setError(e); } };
  return <div className="page home-page">
    <div className="page-title"><div><h1>全部研究</h1><p>从一个问题开始，让研究有序向前。</p></div><Link to="/research/new" className="button primary"><Plus size={15}/>新建研究</Link></div>
    <section className="start-panel">
      <div className="start-copy"><span className="section-kicker"><FlaskConical size={16}/>研究起点</span><h2>下一个值得探索的问题是什么？</h2><p>多条研究线并行探索，从方案讨论到本机实验，保留每一步依据。</p></div>
      <form className="topic-entry" onSubmit={e => { e.preventDefault(); navigate('/research/new', { state: { topic } }); }}><label htmlFor="topic-home" className="sr-only">研究题目</label><input id="topic-home" value={topic} onChange={e => setTopic(e.target.value)} placeholder="输入研究题目，例如：比较不同优化器的收敛行为" maxLength={1000} /><button type="submit" aria-label="创建研究"><ArrowRight size={20}/></button></form>
      <div className="research-route"><span>提出假设</span><ChevronRight size={13}/><span>交叉审查</span><ChevronRight size={13}/><span>本机实验</span><ChevronRight size={13}/><span>证据验证</span><ChevronRight size={13}/><span>阶段报告</span></div>
    </section>
    {records.length === 0 && <section className="setup-strip"><Link to="/settings/providers"><span className="step-number">{ready ? <Check size={15}/> : '1'}</span><div><strong>连接研究模型</strong><span>{ready ? '模型凭据已配置' : '配置 DeepSeek API Key'}</span></div><ArrowUpRight size={15}/></Link><Link to="/settings/team"><span className="step-number">2</span><div><strong>组建研究团队</strong><span>研究线、成员与汇报周期</span></div><ArrowUpRight size={15}/></Link><Link to="/settings/resources"><span className="step-number">3</span><div><strong>检查本机资源</strong><span>CPU、内存与实验环境</span></div><ArrowUpRight size={15}/></Link></section>}
    <section className="research-list">
      <div className="section-heading"><h2>研究项目 <span className="count">{filter === 'deleted' ? records.length - all.length : all.length}</span></h2><span className="muted small">{active.length} 个进行中</span></div>
      <div className="list-toolbar"><div className="tabs">{[['all', '全部'], ['active', '进行中'], ['finished', '已结束'], ['deleted', '已删除']].map(([key, text]) => <button key={key} className={filter === key ? 'active' : ''} onClick={() => setFilter(key)}>{text}</button>)}</div><div className="search"><Search size={15}/><input aria-label="搜索研究" placeholder="搜索任务名称" value={search} onChange={e => setSearch(e.target.value)}/></div></div>
      <ErrorNotice error={error || projects.error} retry={() => { setError(undefined); void projects.refetch(); }}/>
      {pendingDelete && <div className="record-delete-confirm" role="alert"><div><strong>删除“{pendingDelete.title}”的记录？</strong><p>移到“已删除”列表，之后可以恢复。研究文件会保留。</p></div><div className="record-actions"><button className="button" disabled={!!changing} onClick={() => setPendingDelete(undefined)}>取消</button><button className="button danger" disabled={!!changing} onClick={() => void changeVisibility(pendingDelete, true)}>{changing ? '正在删除…' : '删除记录'}</button></div></div>}{projects.isPending ? <div className="loading-rows"><span/><span/><span/></div> : visible.length ? <div className="project-table"><div className="project-row table-heading"><span>任务名称</span><span>状态</span><span>研究线 / 模式</span><span>创建时间</span><span/></div>{visible.map(p => <div className="project-row" key={projectId(p)}><Link className="project-name" to={`/research/${projectId(p)}`}><strong>{p.title || p.topic}</strong><span className="mono">{projectId(p).slice(0, 18)}</span></Link><div><Status value={stops[projectId(p)] ? stops[projectId(p)].state === 'terminated' ? 'terminated' : 'terminating' : p.status}/>{stops[projectId(p)] && <small className="stop-list-note">{stops[projectId(p)].unconfirmed ? '停止状态暂未确认' : stops[projectId(p)].unfinished?.length ? '正在核验进程退出' : stops[projectId(p)].stop_journal_persisted === false ? '停止记录未持久化' : !stops[projectId(p)].db_synced ? '执行已停止，记录待同步' : '停止记录已同步'}</small>}</div><span>{p.config_snapshot?.team?.n || p.config_snapshot?.n || p.team?.n || '—'} 条 <span className="muted">· {p.mode === 'demo' ? '演示' : '真实'}</span></span><span className="small muted">{time(p.created_at)}</span>{recordActions(p)}</div>)}</div> : <Empty title={search ? '没有匹配的研究项目' : filter !== 'all' ? '这个分类中还没有研究' : '还没有研究项目'}>{search ? '试试其他关键词。' : <>创建第一项研究，团队讨论与实验记录将保存在这里。<Link to="/research/new" className="text-button">开始研究 <ArrowRight size={14}/></Link></>}</Empty>}
    </section>
    <div className="principles"><div><Network size={18}/><span><strong>独立推进的研究线</strong><small>固定角色协作，讨论与实验全程可见。</small></span></div><div><Clock3 size={18}/><span><strong>一个总运行时间</strong><small>暂停仍计时，随时可以停止全部实验。</small></span></div><div><KeyRound size={18}/><span><strong>凭据保存在本机</strong><small>密钥由后端保护，不进入研究上下文。</small></span></div></div>
  </div>;
}
