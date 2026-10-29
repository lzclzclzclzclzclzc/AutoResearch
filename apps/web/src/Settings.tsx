import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Check, Cpu, HardDrive, Loader2, RefreshCw, ShieldCheck, Trash2 } from 'lucide-react';
import { api, items, label, write } from './api';
import type { Data } from './api';
import { Empty, ErrorNotice } from './App';
import { TeamFields, TeamPreview, defaults } from './NewResearch';

export default function Settings() {
  const { section } = useParams();
  return <div className="page settings-page"><div className="page-title"><div><h1>{section === 'team' ? '研究团队' : section === 'providers' ? '模型与凭据' : '计算资源'}</h1><p>{section === 'team' ? '设置研究协作方式。配置会在创建研究时冻结。' : section === 'providers' ? '连接 DeepSeek，让每个角色使用完整的研究运行环境。' : '查看本机环境与实际占用，正式实验由全局 Organizer 安排。'}</p></div></div>
    <nav className="tabs settings-tabs"><Link className={section === 'team' ? 'active' : ''} to="/settings/team">团队配置</Link><Link className={section === 'providers' ? 'active' : ''} to="/settings/providers">模型与凭据</Link><Link className={section === 'resources' ? 'active' : ''} to="/settings/resources">本机资源</Link></nav>
    {section === 'team' ? <TeamSettings/> : section === 'providers' ? <ProviderSettings/> : <Resources/>}
  </div>;
}
function TeamSettings() {
  const q = useQuery({ queryKey: ['team'], queryFn: () => api('/api/settings/team') });
  const client = useQueryClient();
  const [team, setTeam] = useState<Data>(defaults);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<unknown>();
  useEffect(() => { if (q.data) setTeam({ ...defaults, ...(q.data.team || q.data) }); }, [q.data]);
  const save = async () => { setSaving(true); setError(undefined); setSaved(false); try { const payload = Object.fromEntries([...Object.keys(defaults),'version'].filter(k => team[k] !== undefined).map(k => [k,team[k]])); await write('/api/settings/team', payload, 'PUT'); setSaved(true); await client.invalidateQueries({ queryKey: ['team'] }); } catch (e) { setError(e); } finally { setSaving(false); } };
  return <><ErrorNotice error={error || q.error} retry={() => void q.refetch()}/><div className="settings-row"><div className="settings-description"><h2>团队规模与汇报</h2><p>每条研究线都有固定的探索组、审查组、实现者与验证者。Boss、Organizer 和 Paper 负责全局协调。</p><TeamPreview team={team}/></div><div className="settings-panel"><TeamFields value={team} onChange={value => { setTeam(value); setSaved(false); }}/><div className="form-footer">{saved && <span className="saved"><Check size={14}/>已保存，适用于新研究</span>}<button className="button primary" disabled={saving || q.isPending} onClick={() => void save()}>{saving ? <Loader2 size={14} className="spin"/> : null}保存配置</button></div></div></div></>;
}
function ProviderSettings() {
  const q = useQuery({ queryKey: ['providers'], queryFn: () => api('/api/settings/providers') });
  const client = useQueryClient();
  const [data, setData] = useState<Data>({ id: 'deepseek-default', name: 'DeepSeek', model: 'deepseek-flash', base_url: 'https://api.deepseek.com/anthropic' });
  const [key, setKey] = useState('');
  const [error, setError] = useState<unknown>();
  const [feedback, setFeedback] = useState('');
  const [busy, setBusy] = useState('');
  const [deleting, setDeleting] = useState(false);
  useEffect(() => { if (q.data && items(q.data)[0]) setData(items(q.data)[0]); }, [q.data]);
  const action = async (kind: string) => { setBusy(kind); setError(undefined); setFeedback(''); try {
    const id = data.provider_config_id || data.provider_id || data.id || 'deepseek-default';
    if (kind === 'save') { await write(`/api/settings/providers/${id}`, { name: data.name || 'DeepSeek', display_name: data.display_name || 'DeepSeek V4.1 Flash', model: data.model, base_url: data.base_url, version: data.version, ...(key ? { api_key: key } : {}) }, 'PUT'); setKey(''); setFeedback('模型配置已保存'); }
    else if (kind === 'test') { const r = await write(`/api/settings/providers/${id}/test`, {}); setFeedback(r.message || (r.ok || r.success ? '连接测试通过' : JSON.stringify(r))); }
    else { await api(`/api/settings/providers/${id}/credential`, { method: 'DELETE' }); setDeleting(false); setFeedback('凭据已删除，后续模型任务将等待新凭据'); }
    await client.invalidateQueries({ queryKey: ['providers'] });
  } catch (e) { setError(e); } finally { setBusy(''); } };
  return <><ErrorNotice error={error || q.error} retry={() => void q.refetch()}/><div className="settings-row"><div className="settings-description"><h2>DeepSeek 模型</h2><p>全部角色默认使用 DeepSeek V4.1 Flash。请求使用实际模型 ID，运行记录保留提供方返回的模型信息。</p><div className="security-note"><ShieldCheck size={19}/><span>API Key 经 Windows DPAPI 加密保存。浏览器只接收掩码；研究配置、会话与导出均不包含密钥。</span></div></div><div className="settings-panel"><div className="field"><label htmlFor="provider-url">API 地址</label><input id="provider-url" value={data.base_url || ''} onChange={e => setData({ ...data, base_url: e.target.value })}/><small>官方地址为 https://api.deepseek.com/anthropic；AutoDL 可填 https://www.autodl.art/api/v1，保存时自动适配。其他服务需支持 Messages 接口。</small></div><div className="field"><label htmlFor="model">模型 ID</label><input id="model" value={data.model || 'deepseek-flash'} onChange={e => setData({ ...data, model: e.target.value })}/></div><div className="field"><label htmlFor="api-key">API Key <span className="field-hint">{data.credential_mask || data.masked_key || data.credential?.mask || '尚未配置'}</span></label><input id="api-key" type="password" autoComplete="new-password" value={key} onChange={e => setKey(e.target.value)} placeholder="输入新密钥；留空保留已有凭据"/><small>保存后清空输入框，密钥不写入浏览器持久存储。</small></div><ErrorNotice error={error}/>{feedback && <div className="notice" role="status">{feedback}</div>}<div className="form-footer"><button className="button" disabled={!!busy} onClick={() => void action('test')}>{busy === 'test' ? <Loader2 size={14} className="spin"/> : <RefreshCw size={14}/>}测试已保存的连接</button><button className="button primary" disabled={!!busy || !data.model} onClick={() => void action('save')}>{busy === 'save' && <Loader2 size={14} className="spin"/>}保存配置</button></div></div></div><div className="settings-row"><div className="settings-description"><h2>删除凭据</h2><p>删除后，使用此凭据的新任务会阻塞。进行中的研究可显式绑定新的凭据版本。</p></div><div className="settings-panel">{deleting ? <><p>删除 DeepSeek 的本机凭据？</p><div className="form-footer"><button className="button" onClick={() => setDeleting(false)}>取消</button><button className="button danger-text" disabled={!!busy} onClick={() => void action('delete')}>确认删除凭据</button></div></> : <button className="button danger-text" onClick={() => setDeleting(true)}><Trash2 size={14}/>删除已保存的密钥</button>}</div></div></>;
}
function Resources() {
  const q = useQuery({ queryKey: ['resources'], queryFn: () => api('/api/resources'), refetchInterval: 5000 });
  const data = { ...(q.data || {}), ...(q.data?.capacity || {}) };
  const cpu = data.cpu || {}; const memory = data.memory || {};
  const gb = (v: number) => v ? `${(v / 1024 ** 3).toFixed(1)} GB` : '—';
  const allocations = items(data.allocations || []).filter(a=>['queued','granted','starting','running','reconciling','stopping','releasing'].includes(a.state || a.status));
  return <><ErrorNotice error={q.error} retry={() => void q.refetch()}/>{data.available === false && <div className="notice">{data.message || 'Runner 未连接，实验启动门禁关闭。'}</div>}<div className="section-heading"><h2>机器环境</h2><button className="button" onClick={() => void q.refetch()}><RefreshCw size={14}/>刷新</button></div><div className="resource-summary"><div><span><Cpu size={16}/>CPU</span><strong>{cpu.logical_count || cpu.count || data.cpu_count || data.cpu_cores || '—'} <small>逻辑核心</small></strong><p>{cpu.model || data.platform || '本机处理器'}</p></div><div><span><HardDrive size={16}/>内存</span><strong>{gb(memory.total_bytes || memory.total || data.memory_total_bytes)}</strong><p>可用 {gb(memory.available_bytes || memory.available || data.memory_available_bytes)}</p></div><div><span>GPU</span><strong>{(data.gpus || data.gpu || []).length} <small>个设备</small></strong><p>{(data.gpus || []).map((g: Data) => g.name).join('、') || '未发现 GPU，可执行 CPU 实验'}</p></div></div><section className="section"><h2>环境检查</h2><dl className="detail-list">{Object.entries(data.environment || { '进程管理': data.process_backend, '磁盘可用': gb(data.disk_free_bytes), 'Runner': data.available ? '已连接' : '不可用' }).filter(([, v]) => v !== undefined).map(([k,v]) => <div key={k}><dt>{k}</dt><dd>{typeof v === 'object' ? JSON.stringify(v) : String(v)}</dd></div>)}</dl></section><section className="section"><h2>当前资源安排 <span className="count">{allocations.length}</span></h2>{allocations.length ? <div className="record-list">{allocations.map((a: Data) => <div key={a.id || a.allocation_id}><span className="mono">{a.run_id || a.allocation_id || a.id}</span><span>{label(a.status || a.state)}</span><span>{a.cpu_cores || a.resources?.cpu_cores || 1} CPU</span></div>)}</div> : <Empty title="当前没有资源分配">实验申请通过后返回原 Implementation，资源在确认进程退出后释放。</Empty>}</section></>;
}
