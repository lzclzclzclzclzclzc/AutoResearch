import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import ts from '../apps/web/node_modules/typescript/lib/typescript.js';
import React from '../apps/web/node_modules/react/index.js';
import { renderToStaticMarkup } from '../apps/web/node_modules/react-dom/server.node.js';
import { discussionVote, voteBallots, voteSearchText, voteStatus } from '../apps/web/src/votes.ts';

const absolute = path => pathToFileURL(fileURLToPath(new URL(path, import.meta.url))).href;
const source = readFileSync(new URL('../apps/web/src/components/DiscussionVote.tsx', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { jsx: ts.JsxEmit.ReactJSX, module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText
  .replace(/import ['"]\.\/DiscussionVote\.css['"];?/, '')
  .replace(/from ['"]\.\.\/votes['"]/, `from '${absolute('../apps/web/src/votes.ts')}'`)
  .replace(/from ['"]react\/jsx-runtime['"]/, `from '${absolute('../apps/web/node_modules/react/jsx-runtime.js')}'`);
const { default: DiscussionVote } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`);
const members = ['line-01/critic/member-01', 'line-01/critic/member-02'];
const ordinary = {id:'poll-1',type:'ordinary',phase:'result',question:'如何处理重复文本？',members,
  options:[{id:'o1',label:'分组交叉验证'},{id:'o2',label:'增加重复样本对照'}],
  ballots:[{member_id:members[0],option:'o1',reason:'避免同一文本跨折。'},{member_id:members[1],option:'o1',reason:'更适合泛化比较。'}]};
const end = {id:'poll-2',type:'end',phase:'result',status:'failed',members,failed_end_votes:1,freeze_remaining:members,
  final_output:{content_markdown:'预先固定分组划分和评估方法。'},
  ballots:[{member_id:members[0],approve:true,next_step:'implementation'}, {member_id:members[1],approve:false,next_step:'exploration',reason:'should not display'}]};
const markup = vote => renderToStaticMarkup(React.createElement(DiscussionVote, {vote,markdown:content=>React.createElement('p',null,content)}));

test('ordinary unanimous results remain discussion material, with reasons and zero option counts', () => {
  const html = markup(ordinary);
  for (const text of ['普通投票','继续讨论','2 票','0 票','避免同一文本跨折。']) assert.ok(html.includes(text), text);
  assert.ok(!html.includes('全票通过'));
});
test('end vote shows yes/no, routing, immutable output and frozen members without requiring reasons', () => {
  const html = markup(end);
  for (const text of ['结束投票','未通过','同意 1 · 不同意 1','交回 Exploration','累计失败 1 / 3','恢复结束投票前','完整产物']) assert.ok(html.includes(text), text);
  assert.ok(!html.includes('should not display'));
});
test('only registered members count once; unfinished polls show missing ballots honestly', () => {
  const vote = {...end,phase:'ballot',ballots:[...end.ballots.slice(0,1),...end.ballots.slice(0,1),{member_id:'intruder',approve:true}]};
  assert.equal(voteBallots(vote).length,1);
  assert.ok(markup(vote).includes('已收 1 / 2 票'));
  assert.ok(markup(vote).includes('等待 critic / member-02'));
});
test('legacy votes remain legacy; route search only matches routes actually recorded', () => {
  assert.equal(discussionVote({kind:'vote',structured:{approve:true}}),undefined);
  const text = voteSearchText({kind:'discussion_vote',vote:end});
  assert.ok(text.includes('交回 Exploration'));
  assert.ok(!text.includes('结束项目'));
  assert.equal(voteStatus({...end,phase:'freeze',freeze_remaining:[]}),'结束投票已恢复');
  assert.ok(voteStatus({...end,phase:'god',failed_end_votes:3}).includes('God'));
});

if (process.argv.includes('--preview')) {
  const folder = new URL('../.test-data/',import.meta.url);
  mkdirSync(folder,{recursive:true});
  const css = readFileSync(new URL('../apps/web/src/components/DiscussionVote.css',import.meta.url),'utf8');
  writeFileSync(new URL('discussion-votes.html',folder),`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>投票记录验收</title><link rel="stylesheet" href="../apps/web/src/styles.css"><style>${css}main{max-width:760px;margin:auto;padding:24px 16px}h1{margin-bottom:20px}.discussion-vote{margin:0 0 20px}</style><main><h1>投票记录验收</h1>${[ordinary,end,{...end,id:'poll-3',phase:'god',failed_end_votes:3,freeze_remaining:[]}].map(markup).join('')}</main></html>`);
}
