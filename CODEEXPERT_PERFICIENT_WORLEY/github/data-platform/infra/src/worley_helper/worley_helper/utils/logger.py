import sys
from datetime import datetime
import pytz
import logging
from worley_helper.utils.constants import TIMEZONE_UTC, TIMESTAMP_FORMAT_WITH_UTC


def get_logger(
    logger_name: str, log_level: int = logging.INFO, timezone: str = TIMEZONE_UTC
) -> logging.Logger:
    """
    Returns a configured logger instance.

    Parameters
    ----------
    Args:
        logger_name: str:
            The name of the logger.
        log_level: (int, optional):
            The logging level. Defaults to logging.INFO.
        timezone (str, optional):
            The timezone for log timestamps. Defaults to 'UTC'.

    Returns
    -------
        logging.Logger: A configured logger instance.

    Examples
    -------
    >>> logger = get_logger(__name__)
    >>> logger.info(f"Uploaded data to {bucket_name}/{object_name}")
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Create a CloudWatch log handler
    cloudwatch_log_handler = logging.StreamHandler(sys.stdout)
    cloudwatch_log_handler.setLevel(log_level)

    # Configure the log formatter to use local time
    local_tz = pytz.timezone(timezone)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt=TIMESTAMP_FORMAT_WITH_UTC
    )
    formatter.converter = lambda time: local_tz.fromutc(
        datetime.utcfromtimestamp(time)
    ).timetuple()
    cloudwatch_log_handler.setFormatter(formatter)

    logger.addHandler(cloudwatch_log_handler)

    return logger
