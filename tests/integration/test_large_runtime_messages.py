import asyncio
import json
import sys
import pytest

from backend.adapters.dsh import RuntimeMessageReader, DshRuntimeAdapter
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import RunnerSupervisor
from packages.contracts.research import Proposal


@pytest.mark.asyncio
async def test_adapter_receives_large_unicode_notification_and_final_result(tmp_path, monkeypatch):
    import backend.adapters.dsh as dsh
    text = '灾难推文' * 30000
    output = {'content_markdown': text, 'candidate': {}, 'evidence': []}
    # Build a valid schema payload without relying on optional field defaults.
    output = Proposal.model_validate(output).model_dump()
    fixture = tmp_path / 'wire.jsonl'
    fixture.write_text(json.dumps({'type':'notification','method':'large-test','payload':text}, ensure_ascii=True)+'\n'+json.dumps({'type':'result','final_response':json.dumps(output,ensure_ascii=False),'finish_reason':'completed'},ensure_ascii=True)+'\n',encoding='utf-8')
    original = asyncio.create_subprocess_exec
    async def spawn(*args, **kwargs):
        code = 'import sys,pathlib;sys.stdin.readline();sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes());sys.stdout.buffer.flush()'
        return await original(sys.executable, '-c', code, str(fixture), **kwargs)
    monkeypatch.setattr(dsh.asyncio, 'create_subprocess_exec', spawn)
    monkeypatch.setattr(dsh, 'runtime_availability', lambda: {'available':True})
    class Credentials:
        def get_secret(self,*args,**kwargs):return 'local-fixture-secret'
    store=Store(tmp_path/'data')
    project=store.start_project(store.create_project({'client_request_id':'large-wire','topic':'LOCAL FIXTURE','mode':'live','max_runtime_seconds':60,'team':{'n':1}})['id'])
    control=ControlSupervisor(tmp_path/'control');control.register_project(project['id'],project['deadline_at'])
    runner=RunnerSupervisor(tmp_path/'data',control=control)
    adapter=DshRuntimeAdapter(store,control,Credentials(),runner=runner)
    try:
        result=await asyncio.wait_for(adapter.execute_turn(research_id=project['id'],agent_id='boss/member-01',role='boss',task_id='large',turn_id='first',prompt='LOCAL FIXTURE',schema=Proposal),timeout=15)
        assert result['content_markdown']==text
        execution=store.list('execution',project['id'])[0]
        assert execution['status']=='completed'
        assert not control.status(project['id'])['unfinished']
        events=store.events(project['id'],limit=100)
        assert any(e['type']=='agent.runtime_event' and e['payload']['notification'].get('payload')==text for e in events)
    finally:
        await adapter.close();runner.close();control.close()



@pytest.mark.asyncio
async def test_message_larger_than_16_mib_has_no_fixed_length_cap():
    process = await asyncio.create_subprocess_exec(sys.executable, '-c',
        "import sys;sys.stdout.buffer.write(b'x'*(18*1024*1024)+b'\\nnext\\n');sys.stdout.buffer.flush()",
        stdout=asyncio.subprocess.PIPE)
    try:
        reader = RuntimeMessageReader(process.stdout)
        first = await asyncio.wait_for(reader.readline(), 15)
        assert first == b'x' * (18 * 1024 * 1024) + b'\n'
        assert await reader.readline() == b'next\n'
        assert await reader.readline() == b''
        await process.wait()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.asyncio
async def test_partial_message_survives_timeout_for_stop_checks():
    stream = asyncio.StreamReader()
    reader = RuntimeMessageReader(stream)
    stream.feed_data(b'{"part":"')
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(reader.readline(), 0.01)
    stream.feed_data(b'complete"}\nsecond\n')
    stream.feed_eof()
    assert await reader.readline() == b'{"part":"complete"}\n'
    assert await reader.readline() == b'second\n'
    assert await reader.readline() == b''


@pytest.mark.asyncio
async def test_default_reader_reproduces_observed_failure():
    stream = asyncio.StreamReader()
    stream.feed_data(b'x' * 70000 + b'\n')
    stream.feed_eof()
    with pytest.raises(ValueError, match='limit'):
        await stream.readline()
