export type Data = Record<string, any>;

let capability: string | undefined;
let bootstrap: Promise<void> | undefined;
export const requestId = () => crypto.randomUUID();

export async function api<T = Data>(path: string, init?: RequestInit, sessionRetry = true): Promise<T> {
  if (!bootstrap) {
    bootstrap = fetch('/api/session', { credentials: 'same-origin' }).then(async response => {
      if (response.ok) {
        const body = await response.json();
        capability = body.csrf_token || body.capability || body.token;
      }
    }).catch(() => undefined);
  }
  await bootstrap;
  const headers = new Headers(init?.headers);
  if (init?.body) headers.set('Content-Type', 'application/json');
  if (capability) { headers.set('X-CSRF-Token', capability); headers.set('X-Local-Capability', capability); }
  if (init?.method && init.method !== 'GET') headers.set('Idempotency-Key', requestId());
  const response = await fetch(path, { ...init, headers, credentials: 'same-origin' });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    const detail = error.detail || error;
    if (sessionRetry && (response.status === 401 || (response.status === 403 && detail.code === 'csrf_rejected'))) {
      capability = undefined; bootstrap = undefined;
      return api<T>(path, init, false);
    }
    throw new Error(typeof detail === 'string' ? detail : detail.message || (Array.isArray(detail) ? detail.map((x: Data) => x.msg).join('；') : `请求失败 (${response.status})`));
  }
  if (response.status === 204) return {} as T;
  return response.json();
}
export const write = <T = Data>(path: string, value: Data, method = 'POST') => api<T>(path, {
  method, body: JSON.stringify({ ...value, client_request_id: value.client_request_id || requestId() }),
});
export function items(value: any): Data[] {
  return Array.isArray(value) ? value : value?.items || value?.projects || value?.conversations || value?.messages || value?.providers || value?.nodes || value?.events || [];
}
export const projectId = (p: Data) => p.research_id || p.id;
export const label = (value?: string) => ({
  idle: '待命', created: '待启动', queued: '排队中', running: '运行中', waiting: '等待中',
  blocked: '需要处理', pausing: '暂停中', paused: '已暂停', terminating: '正在终止', terminated: '已终止',
  completed: '已完成', failed: '失败', cancelled: '已取消', interrupted:'已中断', verifying: '验证中',
  exploring: '探索中', criticizing: '审查中', implementing: '实现中', awaiting_boss: '等待 Boss',
  exploration:'探索中',critic:'审查中',implementation:'实现中',organizer:'资源安排中',verify:'验证中',waiting_boss:'等待 Boss',waiting_resources:'等待资源',
  verification_pending: '待验证', passed: '通过', rejected: '未通过', pending: '待处理',
  arbitrating:'God 正在仲裁',waiting_subtasks:'正在执行拆分任务',waiting_recovery:'等待自动恢复',
  retry_wait:'暂时故障，自动重试中',waiting_dependencies:'等待内部依赖，由 Boss 协调',all_lines_blocked:'所有未完成研究线均已阻塞',
  live: '真实运行', demo: '演示模式', stopped: '执行已停止', quarantined: '已隔离',
  unknown:'状态待核验',registered:'已登记',reconciling:'正在对账',early_stopped:'提前停止',execution_failed:'执行失败',
  no_improvement:'无提升',negative:'负结果',inconclusive:'尚无定论',improvement:'有提升',
  user_requested:'用户立即终止',time_limit_reached:'总运行时间已到期',control_restarted:'控制服务重启，旧研究已隔离',
  launcher_shutdown:'启动器已关闭',credential_missing:'尚未配置有效模型凭据',credential_invalid:'模型凭据无效',
}[value || ''] || value || '—');
export const time = (value?: string) => value ? new Date(value).toLocaleString('zh-CN', { hour12: false }) : '—';
export const duration = (seconds: number) => {
  const s = Math.max(0, Math.floor(seconds));
  return `${Math.floor(s / 3600).toString().padStart(2, '0')}:${Math.floor(s / 60 % 60).toString().padStart(2, '0')}:${(s % 60).toString().padStart(2, '0')}`;
};
export const terminal = (status: string) => ['terminated', 'completed', 'failed', 'cancelled', 'quarantined'].includes(status);
