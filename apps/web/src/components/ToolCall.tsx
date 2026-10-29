import { useState } from 'react';
import { ChevronRight } from 'lucide-react';
import type { Data } from '../api';
import './ToolCall.css';

const formatted = (value: unknown) => typeof value === 'string' ? value : JSON.stringify(value, null, 2);

export function toolSearchText(call: Data) {
  return [call.tool_name, formatted(call.arguments), formatted(call.result)].join('\n');
}

export default function ToolCall({ call }: { call: Data }) {
  const [open, setOpen] = useState(false);
  return <details className="tool-call" open={open} onToggle={event => setOpen(event.currentTarget.open)}>
    <summary className="tool-call-heading"><ChevronRight size={14}/><span>工具调用</span><strong>{call.tool_name}</strong></summary>
    {open && <div className="tool-call-content">
      <div className={`tool-call-status ${call.status}`}>{call.status === 'complete' ? '已完成' : call.status === 'failed' ? '调用失败' : '尚无结果'}</div>
      <div className="tool-call-label">参数</div><pre className="tool-call-data">{formatted(call.arguments)}</pre>
      {call.result !== undefined && <div className="tool-result">
        <div className="tool-call-label">调用结果</div><pre className="tool-call-data">{formatted(call.result)}</pre>
      </div>}
    </div>}
  </details>;
}
