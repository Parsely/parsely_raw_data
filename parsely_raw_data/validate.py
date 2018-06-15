from __future__ import print_function

import logging
import pprint
from collections import defaultdict

from six import string_types

from .schema import SCHEMA

"""
Data Pipeline validation functions
"""

SCHEMA_DICT = None
REQ_FIELDS = None
CHECKS = {'req': 'Fields "{}" are required. ({} are present)',
          'size': 'Field "{}" is too large (size limit {})',
          'type': 'Field "{}" should be {}',
          'not_in_schema': 'Field "{}" not in schema. {}'}

log = logging.getLogger(__name__)

def _create_schema_dict():
    global SCHEMA_DICT, REQ_FIELDS

    SCHEMA_DICT = defaultdict(dict)
    for field in SCHEMA:
        conditions = {k: field.get(k) for k, _ in CHECKS.items()}
        if conditions['type'] == object:
            conditions['type'] = dict
        if conditions['type'] == str:
            conditions['type'] = string_types

        SCHEMA_DICT[field['key']] = conditions

    REQ_FIELDS = set([k for k, v in SCHEMA_DICT.items() if v['req']])
_create_schema_dict()


def _handle_warning(check_type, field, value, cond, raise_error=True):
    """If raise, raise an error. Otherwise just log."""
    msg = CHECKS[check_type].format(field, cond)
    if raise_error:
        raise ValueError(msg, value, type(value))
    else:
        log.warn(msg, value, type(value))

    return False


def validate(event, raise_error=True):
    """Checks whether an event matches the given schema.

    :param raise_error: let errors/exceptions bubble up.
    """
    present = REQ_FIELDS.intersection(set(event.keys()))
    if len(present) != len(REQ_FIELDS):
        return _handle_warning('req', list(REQ_FIELDS), '', list(present), raise_error=raise_error)

    for field, value in event.items():
        try:
            field_reqs = SCHEMA_DICT[field]
            check_type = field_reqs['type']
            check_size = field_reqs['size']

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
                                       value,
                                       check_size,
                                       raise_error=raise_error)

        except KeyError as exc:
            return _handle_warning('not_in_schema', field, value, '', raise_error=raise_error)

    return True  # event passes tests


if __name__ == "__main__":
    log.warn = print

    # non schema fields
    d = {k: "test" for k in REQ_FIELDS}
    d['test'] = "test"
    assert validate(d, raise_error=False) != True

    # fields too long
    d = {k: "test" for k in REQ_FIELDS}
    d['utm_term'] = 'd' * 90
    assert validate(d, raise_error=False) != True

    # fields wrong type
    d = {k: "test" for k in REQ_FIELDS}
    d['timestamp_info_nginx_ms'] = 123456
    d['extra_data'] = "not a dict"
    assert validate(d, raise_error=False) != True

    d['visitor'] = "true"
    assert validate(d, raise_error=False) != True

    d['ip_lat'] = 4
    assert validate(d, raise_error=False) != True

    # not all required fields
    d = {}
    assert validate(d, raise_error=False) != True

    # error catching
    d = {}
    err = False
    try:
        validate(d)
    except Exception as e:
        err = True

    assert err == True
