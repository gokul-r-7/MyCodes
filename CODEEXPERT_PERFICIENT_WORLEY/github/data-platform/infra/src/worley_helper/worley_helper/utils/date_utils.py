from datetime import datetime, timedelta
import pytz
from worley_helper.utils.constants import TIMEZONE_SYDNEY, SNAPSHOT_DATE_FORMAT, AUDIT_DATE_ISO8601_FORMAT


def generate_snapshot_date_string(timezone=TIMEZONE_SYDNEY):
    """
    Get snapshot date for today
    :return: Snapshot date string
    """
    timestamp = generate_timestamp_string(timezone)
    date_str = timestamp.strftime(SNAPSHOT_DATE_FORMAT)
    return date_str


def generate_today_date_string(
    timezone=TIMEZONE_SYDNEY, output_date_format: str = SNAPSHOT_DATE_FORMAT
):
    """
    Get date for Today
    :return: Snapshot date string
    """
    timestamp = generate_timestamp_string(timezone)
    date_str = timestamp.strftime(output_date_format)
    return date_str


def generate_yesterday_date_string(
    timezone=TIMEZONE_SYDNEY, output_date_format: str = SNAPSHOT_DATE_FORMAT
):
    """
    Get snapshot date for yesterday
    :return: Snapshot date string
    """
    timestamp = generate_timestamp_string(timezone)
    yesterday = timestamp - timedelta(days=1)
    date_str = yesterday.strftime(output_date_format)
    return date_str


def generate_formatted_snapshot_date_string(
    date: str, date_format: str, input_date_format: str = "%Y%m%d"
):
    """
    Get snapshot date with given date string and format
    :param date: date as a string
    :param date_format: date format
    :param input_date_format: date format for the input date
    :return: snapshot date
    """
    parsed_date = datetime.strptime(date, input_date_format)
    snapshot_date = parsed_date.strftime(date_format)
    return snapshot_date


def generate_timestamp_string(timezone: str = None):
    """
    Get timestamp for now
    :return: Timestamp
    """
    tz = pytz.UTC if timezone is None or timezone is "" else pytz.timezone(timezone)
    timestamp = datetime.now(tz=tz)
    return timestamp


def generate_timestamp_iso8601_date_format_string(
        timezone=TIMEZONE_SYDNEY, output_date_format: str = AUDIT_DATE_ISO8601_FORMAT
):
    """
    Get date for Today
    :return: ISO 8601 Extended Format with Milliseconds date
    """

    timestamp = generate_timestamp_string(timezone)

    timestamp = timestamp.strftime(output_date_format)

    return timestamp