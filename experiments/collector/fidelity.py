"""Independent oracle for the bounded usage fixture projection (not all OTLP)."""
from collections import Counter


def decode_value(value):
    kind = value.WhichOneof('value')
    if kind == 'kvlist_value':
        pairs = value.kvlist_value.values
        result = {pair.key: decode_value(pair.value) for pair in pairs}
        if len(result) != len(pairs):
            raise ValueError('duplicate OTLP body key')
        return result
    if kind == 'array_value':
        return [decode_value(item) for item in value.array_value.values]
    if kind in {'string_value', 'bool_value', 'int_value', 'double_value'}:
        return getattr(value, kind)
    raise ValueError(f'unsupported fixture value: {kind}')


def projection(record):
    attributes = {pair.key: decode_value(pair.value) for pair in record.attributes}
    if len(attributes) != len(record.attributes):
        raise ValueError('duplicate OTLP attribute')
    return {'body': decode_value(record.body), 'timestamp_ns': str(record.time_unix_nano),
            'event_name': record.event_name, 'attributes': attributes}


def check_fidelity(expected, evidence, observed, *, allow_retries=False):
    """Compare identity, complete bodies and explicit fixture projection fields."""
    if evidence != expected:
        raise AssertionError('local evidence differs from input, including order/multiplicity')
    wanted = {event['event_id']: event for event in expected}
    if len(wanted) != len(expected):
        raise ValueError('fidelity inputs require distinct event IDs')
    counts = Counter(record['body']['event_id'] for record in observed)
    if set(counts) != set(wanted):
        raise AssertionError('missing or unexpected delivered event')
    if not allow_retries and any(count != 1 for count in counts.values()):
        raise AssertionError('unexpected duplicate delivery')
    for record in observed:
        event = wanted[record['body']['event_id']]
        assert record['body'] == event, 'event body changed'
        assert record['timestamp_ns'] == event['time_unix_nano'], 'event timestamp changed'
        assert record['event_name'] == event['event_type'], 'event name changed'
        # Deliberately independent of the SDK projection implementation.
        attrs = {'agentrust.telemetry.spec_version': event['spec_version'],
                 'agentrust.event.id': event['event_id'], 'agentrust.run.id': event['run_id'],
                 'agentrust.workflow.id': event['workflow_id'],
                 'gen_ai.agent.id': event['agent_id'], 'agentrust.usage.scope': event['scope']}
        assert record['attributes'] == attrs, 'fixture attributes changed'
    delivered = [record['body']['event_id'] for record in observed]
    first_seen = list(dict.fromkeys(delivered))
    return {'unique_delivered': len(counts), 'duplicate_attempts': sum(counts.values()) - len(counts),
            'reordered': first_seen != list(wanted)}
