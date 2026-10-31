import type { ReactNode } from 'react';
import type { Data } from '../api';
import { memberLabel, nextStepLabel, voteBallots, voteOptionLabel, voteStatus } from '../votes';
import './DiscussionVote.css';

export default function DiscussionVote({ vote, markdown }: { vote: Data; markdown: (content: string) => ReactNode }) {
  const end = vote.type === 'end';
  const ballots = voteBallots(vote);
  const displayedBallots = vote.phase === 'ballot' && vote.ballot ? [vote.ballot] : ['freeze', 'god'].includes(vote.phase) ? [] : ballots;
  const members: string[] = vote.members || [];
  const remaining: string[] = vote.freeze_remaining || [];
  const pending = members.filter(member => !ballots.some(ballot => ballot.member_id === member));
  const finalOutput = vote.final_output;
  const complete = vote.phase === 'result';
  const approved = ballots.filter(b => b.approve === true).length;
  const rejected = ballots.filter(b => b.approve === false).length;
  return <section className="discussion-vote" aria-label={end ? '结束投票' : '普通投票'}>
    <div className="vote-heading"><strong>{end ? '结束投票' : '普通投票'}</strong><span className={complete && end ? vote.status === 'passed' ? 'vote-passed' : 'vote-failed' : ''}>{voteStatus(vote)}</span></div>
    <p className="vote-question">{vote.question || (end ? '是否同意以这份产物结束当前讨论并推进？' : '讨论议题')}</p>
    {end && <p className="vote-rule">全体成员同意，且推进方向一致后通过。</p>}
    {!end && vote.options?.length > 0 && <ul className="vote-options">{vote.options.map((option: Data) => <li key={option.id || option.label}>
      <span>{option.label}</span>{complete && <b>{ballots.filter(b => b.option === option.id || b.option === option.label).length} 票</b>}
    </li>)}</ul>}
    {displayedBallots.length > 0 && <ul className="vote-ballots" aria-label="成员投票记录">{displayedBallots.map(ballot => <li key={ballot.member_id}>
      <div><span title={ballot.member_id}>{memberLabel(ballot.member_id)}</span><strong className={end && ballot.approve === false ? 'vote-failed' : ''}>{end ? ballot.approve === true ? '同意' : ballot.approve === false ? '不同意' : '等待有效票' : voteOptionLabel(vote, ballot.option)}</strong></div>
      {end ? ballot.next_step && <p>{nextStepLabel(ballot.next_step)}</p> : ballot.reason && <p>{ballot.reason}</p>}
    </li>)}</ul>}
    {end && complete && <p className="vote-tally">同意 {approved} · 不同意 {rejected}{vote.next_step ? ` · ${nextStepLabel(vote.next_step)}` : ''}</p>}
    {end && complete && rejected === 0 && new Set(ballots.map(b => b.next_step)).size > 1 && <p className="vote-failed vote-note">全员同意，但推进方向不一致，本次未通过。</p>}
    {members.length > 0 && ['started', 'ballot'].includes(vote.phase) && <p className="vote-note">已收 {ballots.length} / {members.length} 票{pending.length > 0 ? ` · 等待 ${pending.map(memberLabel).join('、')}` : ''}</p>}
    {end && Number.isInteger(vote.failed_end_votes) && <p className="vote-note">本次讨论结束投票累计失败 {vote.failed_end_votes} / 3 次</p>}
    {remaining.length > 0 && <p className="vote-freeze">恢复结束投票前，还需发言：{remaining.map(memberLabel).join('、')}</p>}
    {vote.phase === 'freeze' && remaining.length === 0 && <p className="vote-note">全体成员已完成新一轮发言，可以再次发起结束投票。</p>}
    {vote.phase === 'god' && <p className="vote-note">{vote.status === 'finalized' ? `God 已定稿 · ${nextStepLabel(vote.next_step)}` : '由 God 输出完整最终产物和推进方向，随后直接交接。'}</p>}
    {vote.diagnostic && <p className="vote-failed vote-note">{vote.diagnostic}</p>}
    {finalOutput && <details className="vote-output"><summary>查看本次绑定的完整产物</summary>
      {finalOutput.content_markdown ? markdown(finalOutput.content_markdown) : typeof finalOutput === 'string' ? markdown(finalOutput) : null}
      {typeof finalOutput === 'object' && Object.keys(finalOutput).some(key => key !== 'content_markdown') && <pre>{JSON.stringify(Object.fromEntries(Object.entries(finalOutput).filter(([key]) => key !== 'content_markdown')), null, 2)}</pre>}
    </details>}
  </section>;
}
