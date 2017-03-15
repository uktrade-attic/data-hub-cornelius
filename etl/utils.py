import re
import time
import datetime
import dateutil.parser


DATETIME_RE = re.compile('/Date\(([-+]?\d+)\)/')


def cdms_datetime_to_datetime(value):
    """
    Parses a cdms datetime as string and returns the equivalent datetime value.
    Dates in CDMS are always UTC.
    """
    if isinstance(value, datetime.datetime):
        return value
    match = DATETIME_RE.match(value or '')
    if match:
        parsed_val = int(match.group(1))
        parsed_val = datetime.datetime.utcfromtimestamp(parsed_val / 1000)
        return parsed_val.replace(tzinfo=datetime.timezone.utc)
    else:
        return value


def datetime_to_cdms_datetime(value):
    """
    Returns the cdms string equivalent of the datetime value.
    """
    if not isinstance(value, datetime.datetime):
        value = dateutil.parser.parse(value)
    return '/Date({0})/'.format(
        int(time.mktime(value.timetuple()) * 1000))
