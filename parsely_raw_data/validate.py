from __future__ import print_function

import logging
import pprint
from collections import defaultdict

from six import string_types

from .schema import SCHEMA, mk_sample_event

"""
Data Pipeline validation functions
"""

SCHEMA_DICT = None
REQ_FIELDS = None
CHECKS = {'req': 'Fields "{}" are missing, but required. \n{} present',
          'size': 'Field "{}" is too large (size limit {})',
          'type': 'Field "{}" should be {}',
          'not_in_schema': 'Field "{}" not in schema. {}',}

log = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    """An event fails the validation test."""
    pass


def _create_schema_dict():
    global SCHEMA_DICT, REQ_FIELDS

    SCHEMA_DICT = defaultdict(dict)
    for field_conditions in SCHEMA:
        conditions = field_conditions
        if conditions['type'] == object:
            conditions['type'] = dict
        if conditions['type'] == str:
            conditions['type'] = string_types

        SCHEMA_DICT[field_conditions['key']] = conditions

    # all required and top-level fields should be present.
    REQ_FIELDS = set([k for k, v in SCHEMA_DICT.items() if v.get('req') or not v.get('available_with_field')])
    SCHEMA_DICT = dict(SCHEMA_DICT)
_create_schema_dict()


def _handle_warning(check_type, field, value, cond, raise_error=True):
    """If raise, raise an error. Otherwise just log."""
    msg = "Validation Error:  " + CHECKS[check_type].format(field, cond)
    if raise_error:
        raise SchemaValidationError(msg, value, type(value))
    else:
        log.warn(msg, value, type(value))

    return False


def validate(event, raise_error=True):
    """Checks whether an event matches the given schema.

    :param raise_error: let errors/exceptions bubble up.
    """
    present = REQ_FIELDS.intersection(set(event.keys()))
    if len(present) != len(REQ_FIELDS):
        missing = REQ_FIELDS - present
        return _handle_warning('req', list(missing), '', list(present), raise_error=raise_error)

    for field, value in event.items():
        try:
            field_reqs = SCHEMA_DICT[field]
            check_type = field_reqs.get('type')
            check_size = field_reqs.get('size')

            # verify type based on schema
            if value is not None and not isinstance(value, check_type):
                return _handle_warning('type',
                                       field,
                                       value,
                                       check_type,
                                       raise_error=raise_error)

            # verify size of string values
            if isinstance(value, string_types) and check_size is not None and len(value) > check_size:
                return _handle_warning('size',
                                       field,
                                       value[:10] + '... is len({})'.format(len(value)),
                                       check_size,
                                       raise_error=raise_error)

        except KeyError as exc:
            return _handle_warning('not_in_schema', field, value, '', raise_error=raise_error)

    return True  # event passes tests


if __name__ == "__main__":
    log.warn = print

    # non schema fields
    d = {k: SCHEMA_DICT[k]['ex'] for k in REQ_FIELDS}
    d['test'] = "test"
    assert validate(d, raise_error=False) != True
    del d['test']

    # fields too long
    d['utm_term'] = 'd' * 90
    assert validate(d, raise_error=False) != True
    d['utm_term'] = SCHEMA_DICT['utm_term']['ex']

    # fields wrong type
    d['extra_data'] = "not a dict"
    assert validate(d, raise_error=False) != True
    d['extra_data'] = {}

    d['visitor'] = "true"
    assert validate(d, raise_error=False) != True
    d['visitor'] = True

    d['ip_lat'] = 4
    assert validate(d, raise_error=False) != True
    d['ip_lat'] = 4.0

    # not all required fields
    d = {'apikey': 'test.com'}
    assert validate(d, raise_error=False) != True

    # error catching
    d = {}
    err = False
    try:
        validate(d)
    except Exception as e:
        err = True

    assert err == True

    # verify the examples
    example = mk_sample_event()
    assert validate(example, raise_error=False) == True
