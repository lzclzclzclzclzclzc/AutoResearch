import { useEffect, useRef, useState } from 'react';
import type { CSSProperties, ReactNode } from 'react';
import './WorkspacePanels.css';

const preferenceKey = 'autoresearch.chat-width';
const defaultWidth = 360;
const clamp = (width: number, max: number) => Math.max(280, Math.min(width, max));

export default function WorkspacePanels({ mobilePane, children }: { mobilePane: string; children: ReactNode }) {
  const container = useRef<HTMLDivElement>(null);
  const drag = useRef<{ x: number; width: number } | null>(null);
  const [preferred, setPreferred] = useState(() => {
    try { const stored = Number(localStorage.getItem(preferenceKey)); return Number.isFinite(stored) && stored >= 280 ? stored : defaultWidth; }
    catch { return defaultWidth; }
  });
  const [available, setAvailable] = useState(800);
  const [dragging, setDragging] = useState(false);
  const max = Math.max(280, Math.min(800, available - 370));
  const width = clamp(preferred, max);
  const lastWidth = useRef(width);
  useEffect(() => {
    const element = container.current;
    if (!element) return;
    const observer = new ResizeObserver(() => setAvailable(element.getBoundingClientRect().width));
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  function change(next: number, persist = false) {
    const value = clamp(next, max);
    lastWidth.current = value;
    setPreferred(value);
    if (persist) { try { localStorage.setItem(preferenceKey, String(value)); } catch { /* The layout also works without persistent storage. */ } }
  }
  function finish() {
    if (!drag.current) return;
    drag.current = null;
    setDragging(false);
    change(lastWidth.current, true);
  }
  const panels = Array.isArray(children) ? children : [children];
  return <div ref={container} className={`workbench-grid resizable-workbench show-${mobilePane}${dragging ? ' is-resizing' : ''}`} style={{ '--chat-width': `${width}px` } as CSSProperties}>
    {panels[0]}
    <div className="workbench-resizer" role="separator" tabIndex={0} aria-label="调整团队交流宽度" aria-orientation="vertical" aria-controls="team-chat"
      aria-valuemin={280} aria-valuemax={Math.round(max)} aria-valuenow={Math.round(width)} aria-valuetext={`${Math.round(width)} 像素`}
      title="左右拖动调整交流栏宽度；方向键微调，双击恢复默认"
      onPointerDown={event => {
        if (event.button !== 0) return;
        event.preventDefault(); event.currentTarget.focus(); event.currentTarget.setPointerCapture(event.pointerId);
        lastWidth.current = width; drag.current = { x: event.clientX, width }; setDragging(true);
      }}
      onPointerMove={event => { if (drag.current) change(drag.current.width + drag.current.x - event.clientX); }}
      onPointerUp={finish} onPointerCancel={finish} onLostPointerCapture={finish}
      onDoubleClick={() => change(defaultWidth, true)}
      onKeyDown={event => {
        const next = { ArrowLeft: width + 24, ArrowRight: width - 24, Home: 280, End: max }[event.key];
        if (next === undefined) return;
        event.preventDefault(); change(next, true);
      }}/>
    {panels.slice(1)}
  </div>;
}
