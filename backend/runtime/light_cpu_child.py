"""Disposable calculation child: files are primary; JSON is an optional preview."""
import csv
import json
import reprlib
import runpy
import sys
from pathlib import Path


class EvidenceFile(dict):
    def __getitem__(self, key):
        # Compatibility convenience, evaluated only if the script requests it.
        if key == 'data' and key not in self:
            path = Path(super().__getitem__('path'))
            if path.suffix.lower() == '.json':
                value = json.loads(path.read_text(encoding='utf-8'))
            elif path.suffix.lower() in {'.csv', '.tsv'}:
                with path.open(encoding='utf-8', newline='') as stream:
                    value = list(csv.DictReader(stream, delimiter='\t' if path.suffix.lower()=='.tsv' else ','))
            else:
                value = path.read_bytes()
                try: value = value.decode('utf-8')
                except UnicodeDecodeError: pass
            self[key] = value
        return super().__getitem__(key)


def scientific_default(value):
    root = type(value).__module__.split('.')[0]
    if root == 'numpy':
        return value.tolist() if hasattr(value, 'tolist') else value.item()
    if root == 'pandas':
        if type(value).__name__ == 'DataFrame': return value.to_dict(orient='records')
        if hasattr(value, 'tolist'): return value.tolist()
        if type(value).__name__ in ('NAType', 'NaTType'): return None
        if hasattr(value, 'isoformat'): return value.isoformat()
    raise TypeError(type(value).__name__)


def main():
    context = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
    scope = runpy.run_path(str(Path(__file__).with_name('analysis.py')), init_globals={
        'evidence': {key: EvidenceFile(value) for key, value in context['evidence'].items()},
        'workspace_dir': context['workspace_dir'], 'data_dir': context['data_dir'], 'result': None}, run_name='__main__')
    value = scope.get('result')
    try:
        encoded = json.dumps({'result': value}, ensure_ascii=False, default=scientific_default)
    except Exception:
        try: preview = reprlib.repr(value)
        except Exception: preview = '<result available in calculation output files>'
        Path(sys.argv[2]).with_suffix('.txt').write_text(preview, encoding='utf-8')
    else:
        Path(sys.argv[2]).write_text(encoded, encoding='utf-8')


if __name__ == '__main__':
    main()
