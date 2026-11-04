"""Model-facing contracts omit metadata supplied by the current program operation."""
from __future__ import annotations

import copy
import json
from packages.contracts.research import digest


def bindings(schema, context):
    name = schema.__name__
    context = context.get('original_context', context)
    if name == 'ExperimentSpec':
        value = {key: context[source] for key, source in
                [('spec_id','spec_id'), ('version','spec_version'), ('deadline_at','deadline_at')] if source in context}
        if context.get('discussion_protocol') == 2:
            value['discussion_protocol'] = 2
        return value
    if name == 'Implementation' and context.get('spec'):
        return {'spec_hash': digest(context['spec'])}
    if name == 'MonitorDecision' and context.get('evidence_log_hash'):
        return {'evidence_log_hash': context['evidence_log_hash']}
    if name == 'GodDecision' and context.get('arbitration_request'):
        request = context['arbitration_request']
        return {'arbitration_id': request.get('arbitration_id', request.get('id')),
                'snapshot_hash': request['snapshot_hash'], 'target_schema_hash': request.get('target_schema_hash')}
    return {}


def bind_metadata(schema, value, context):
    value = copy.deepcopy(value)
    bound = bindings(schema, context)
    if schema.__name__ == 'GodDecision':
        target = bound.pop('target_schema_hash', None)
        if target:
            if value.get('action') == 'repair_output': value['target_schema_hash'] = target
            if value.get('action') == 'decompose_task' and isinstance(value.get('plan'), dict):
                value['plan']['target_schema_hash'] = target
    value.update(bound)
    return value


def model_contract(schema, context, repaired_validator=None):
    bound = bindings(schema, context)
    if not bound and repaired_validator is None: return schema
    document = copy.deepcopy(schema.model_json_schema())
    def omit(node, fields):
        for key in fields: node.get('properties', {}).pop(key, None)
        if 'required' in node: node['required'] = [key for key in node['required'] if key not in fields]
    if schema.__name__ == 'GodDecision':
        for node in document.get('$defs', {}).values():
            omit(node, bound)
    else:
        omit(document, bound)
        if schema.__name__ == 'ExperimentSpec' and context.get('discussion_protocol') == 2:
            optional = {'hypothesis', 'baseline', 'data', 'design', 'monitoring', 'completion_criteria'}
            document['required'] = [key for key in document.get('required', []) if key not in optional]
    class BoundContract:
        @classmethod
        def validate_repaired_output(cls, value):
            if repaired_validator is not None:
                repaired_validator(value)
        @classmethod
        def model_json_schema(cls): return copy.deepcopy(document)
        @classmethod
        def model_validate(cls, value): return schema.model_validate(bind_metadata(schema, value, context))
        @classmethod
        def model_validate_json(cls, text): return cls.model_validate(json.loads(text))
    BoundContract.__name__ = schema.__name__
    return BoundContract
