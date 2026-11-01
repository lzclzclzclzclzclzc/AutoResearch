import { Suspense, lazy, useEffect, useState } from 'react';
import { NavLink, Link, Route, Routes, useLocation } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { Activity, ArrowUpRight, ChevronRight, Cpu, FlaskConical, FolderOpen, Menu, Network, Plus, Settings2, X } from 'lucide-react';
import { api, items } from './api';
import Home from './Home';
const Settings = lazy(() => import('./Settings'));
const Workspace = lazy(() => import('./Workspace'));
const NewResearch = lazy(() => import('./NewResearch'));

export function Mark() { return <img src="/mark.svg" alt="" width="29" height="29" />; }
export function Status({ value }: { value?: string }) {
  const labels: Record<string, string> = { running: '运行中', completed: '已完成', blocked: '需要处理', terminated: '已终止', terminating: '正在终止', paused: '已暂停', queued: '排队中', created: '待启动', failed: '失败', quarantined: '已隔离' };
  return <span className={`status status-${value}`}><i />{labels[value || ''] || value || '待命'}</span>;
}
export function Empty({ title, children }: { title: string; children?: React.ReactNode }) { return <div className="empty"><span>{title}</span>{children && <p>{children}</p>}</div>; }
export function ErrorNotice({ error, retry }: { error: unknown; retry?: () => void }) {
  if (!error) return null;
  return <div className="notice error" role="alert"><span>{error instanceof Error ? error.message : String(error)}</span>{retry && <button className="text-button" onClick={retry}>重试</button>}</div>;
}

export default function App() {
  const [open, setOpen] = useState(false);
  const location = useLocation();
  useEffect(() => { window.scrollTo({ top: 0, left: 0, behavior: 'instant' }); }, [location.pathname]);
  const health = useQuery({ queryKey: ['health'], queryFn: () => api('/api/health'), refetchInterval: 15000 });
  const projects = useQuery({ queryKey: ['projects'], queryFn: () => api('/api/research'), refetchInterval: 5000 });
  const active = items(projects.data).filter(p => ['running', 'queued', 'paused', 'blocked'].includes(p.status)).length;
  return <div className="app-shell">
    <div className="mobile-header"><button className="icon-button" aria-label="打开导航" onClick={() => setOpen(!open)}>{open ? <X size={20} /> : <Menu size={20} />}</button><Link to="/" className="brand"><Mark />AutoResearch</Link><Link to="/research/new" className="icon-button" aria-label="新建研究"><Plus size={20}/></Link></div>
    <aside className={`sidebar ${open ? 'is-open' : ''}`}>
      <Link className="brand" to="/" onClick={() => setOpen(false)}><Mark /><span>AutoResearch<span className="brand-sub">本机科研工作空间</span></span></Link>
      <Link to="/research/new" className="new-research" onClick={() => setOpen(false)}><Plus size={16} />新建研究<span>↗</span></Link>
      <nav onClick={() => setOpen(false)}>
        <span className="nav-label">工作空间</span>
        <NavLink to="/" end><FolderOpen size={17} />全部研究{active > 0 && <span className="nav-count">{active} 进行中</span>}</NavLink>
        <NavLink to="/settings/team"><Network size={17} />研究团队</NavLink>
        <span className="nav-label">本机配置</span>
        <NavLink to="/settings/providers"><Settings2 size={17} />模型与凭据</NavLink>
        <NavLink to="/settings/resources"><Cpu size={17} />计算资源</NavLink>
      </nav>
      <div className="sidebar-foot">
        <div className="local-status"><span className={`connection-dot ${health.isError ? 'off' : ''}`} /><div>本机服务<span>{health.isError ? '连接不可用' : health.data ? '已连接 · 仅本机访问' : '正在连接…'}</span></div><Activity size={15} /></div>
      </div>
    </aside>
    <div className="main-frame">
      <div className="topbar"><div className="breadcrumb"><FlaskConical size={15} /><span>工作空间</span><ChevronRight size={13}/><span>{location.pathname.includes('settings') ? '配置' : location.pathname.includes('/research/') ? '研究' : '全部研究'}</span></div><span className="local-badge"><span/>Local workspace</span></div>
      <main className="sheet">
        <Suspense fallback={<div className="page"><div className="loading-rows" aria-label="页面加载中"><span/><span/></div></div>}><Routes>
          <Route path="/" element={<Home />} />
          <Route path="/research/new" element={<NewResearch />} />
          <Route path="/research/:id" element={<Workspace />} />
          <Route path="/research/:id/history" element={<Workspace history />} />
          <Route path="/settings/:section" element={<Settings />} />
          <Route path="*" element={<div className="page"><h1>页面未找到</h1><Link to="/">返回研究列表 <ArrowUpRight size={14}/></Link></div>} />
        </Routes></Suspense>
      </main>
    </div>
  </div>;
}
