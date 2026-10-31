import type { Data } from './api';

export const nextStepLabels: Record<string, string> = {
  critic: '交给 Critic', implementation: '交给 Implementation', exploration: '交回 Exploration',
  dispatch: '分派研究任务', finish_lines: '结束选中研究线', finish_project: '结束项目', report: '生成阶段报告',
};
export const memberLabel = (id: string) => id.split('/').slice(-2).join(' / ');
export const nextStepLabel = (step?: string) => step ? nextStepLabels[step] || step : '—';

/** Only explicit v2 metadata is a new ballot. Legacy votes keep their original meaning. */
export function discussionVote(message: Data): Data | undefined {
  const vote = message.vote;
  return message.kind === 'discussion_vote' && vote && ['ordinary', 'end'].includes(vote.type) ? vote : undefined;
}

export function voteBallots(vote: Data): Data[] {
  const members = new Set(vote.members || []);
  return [...new Map((Array.isArray(vote.ballots) ? vote.ballots : [])
    .filter((b: Data) => b.member_id && (!members.size || members.has(b.member_id)))
    .map((b: Data) => [b.member_id, b])).values()] as Data[];
}

export function voteOptionLabel(vote: Data, option: string): string {
  return (vote.options || []).find((o: Data) => o.id === option || o.label === option)?.label || option;
}

export function voteSearchText(message: Data): string {
  const vote = discussionVote(message);
  if (!vote) return message.content_markdown || '';
  return [message.content_markdown, vote.type === 'end' ? '结束投票' : '普通投票', vote.question,
    nextStepLabels[vote.next_step], ...voteBallots(vote).map(b => nextStepLabels[b.next_step]), JSON.stringify(vote)].join('\n');
}

export function voteStatus(vote: Data): string {
  if (vote.phase === 'freeze') return (vote.freeze_remaining || []).length ? '结束投票暂时冻结' : '结束投票已恢复';
  if (vote.phase === 'god') return vote.status === 'technical_failure' ? 'God 技术故障 · 等待恢复' : vote.status === 'finalized' ? 'God 已定稿' : '第三次失败 · God 正在定稿';
  if (vote.phase !== 'result') return vote.phase === 'started' ? '已发起 · 等待成员投票' : '成员已投票';
  if (vote.type === 'ordinary') return '统计完成 · 继续讨论';
  return ['passed', 'approved', 'unanimous'].includes(vote.status) ? '全票通过' : '未通过';
}
