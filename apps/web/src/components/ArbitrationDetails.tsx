import { useQuery } from '@tanstack/react-query';
import { api, time } from '../api';
import type { Data } from '../api';
import { arbitrationActions, arbitrationPhase, arbitrationReasons } from '../arbitrations';
import { Empty, ErrorNotice } from '../App';
import { Markdown } from './Chat';

export default function ArbitrationDetails({ researchId, requests, conversationId, replaySeq, choose }: {
  researchId: string; requests: Data[]; conversationId?: string; replaySeq?: number; choose: (id: string) => void;
}) {
  const selected = requests.find(request => request.conversation_id === conversationId) || requests[0];
  const id = selected?.arbitration_id || selected?.id;
  const detail = useQuery({ queryKey:['arbitration',researchId,id,replaySeq],
    queryFn:() => api(`/api/research/${researchId}/arbitrations/${encodeURIComponent(id)}${replaySeq === undefined ? '' : `?at_seq=${replaySeq}`}`),
    enabled:!!id, refetchInterval:replaySeq === undefined ? 4000 : false });
  const request = detail.data || selected || {};
  const decision = request.decision;
  return <div className="arbitration-details">
    <p className="small muted">每项仲裁使用独立会话；裁决通过校验并应用后才标记为已采用。</p>
    <div className="arbitration-list">{requests.map(request => <button key={request.arbitration_id || request.id}
      className={(request.arbitration_id || request.id) === id ? 'selected' : ''} onClick={() => choose(request.conversation_id)}>
      <strong>{request.scope_id || request.origin_node}</strong><span>{arbitrationPhase(request)}</span>
    </button>)}</div>
    <ErrorNotice error={detail.error} retry={() => void detail.refetch()}/>
    {!selected ? <Empty title="尚无自动仲裁">检测到符合条件的僵局或输出失败后，God 会自动处理。</Empty> : <article>
      <h3>{arbitrationPhase(request)}</h3>
      <dl className="arbitration-facts"><div><dt>原因</dt><dd>{arbitrationReasons[request.reason_code] || request.reason_code}</dd></div>
        <div><dt>原任务</dt><dd>{request.source_task_id || '—'}</dd></div>
        <div><dt>范围</dt><dd>{request.scope_id}</dd></div>
        <div><dt>请求时间</dt><dd>{time(request.created_at)}</dd></div>
        <div><dt>判决时限</dt><dd>{time(request.judge_deadline_at)}</dd></div></dl>
      {decision && <><h3>{arbitrationActions[decision.action] || decision.action} · {request.status === 'applied' ? '已采用' : '尚未应用'}</h3>
        <Markdown content={decision.content_markdown || decision.rationale || ''}/>
        {decision.rationale && decision.rationale !== decision.content_markdown && <p>{decision.rationale}</p>}
        {!!decision.minority_views?.length && <><h4>保留异议</h4><ul>{decision.minority_views.map((view: string,index: number) => <li key={index}>{view}</li>)}</ul></>}
        {!!decision.limitations?.length && <><h4>局限</h4><ul>{decision.limitations.map((view: string,index: number) => <li key={index}>{view}</li>)}</ul></>}
        <details><summary>经过校验的决策与来源</summary><pre>{JSON.stringify({decision,validation:request.validation,source_execution_id:request.source_execution_id},null,2)}</pre></details>
      </>}
      {(request.error || request.diagnostic) && <div className="notice">{typeof (request.error || request.diagnostic) === 'string' ? request.error || request.diagnostic : JSON.stringify(request.error || request.diagnostic)}</div>}
      {!!request.effects?.length && <details><summary>已提交的应用动作</summary><pre>{JSON.stringify(request.effects,null,2)}</pre></details>}
      <details><summary>冻结上下文清单</summary><pre>{JSON.stringify({snapshot_id:request.snapshot_id,snapshot_hash:request.snapshot_hash,sections:request.snapshot_manifest},null,2)}</pre></details>
      <details><summary>尝试与状态历史</summary><pre>{JSON.stringify({attempts:request.attempts,history:request.history},null,2)}</pre></details>
    </article>}
  </div>;
}
