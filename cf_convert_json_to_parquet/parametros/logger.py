import logging
import os
import sys
from enum import IntEnum

from google.cloud import logging as cloudlogging


class LoggingClient:
    log_level = 'DEBUG'

    def __init__(self, level='DEBUG'):
        project = os.environ.get('GCP_PROJECT', "local")
        function_name = os.environ.get('FUNCTION_NAME', "local")
        region = os.environ.get('FUNCTION_REGION', "local")

        log_name = 'projects/{}/logs/cloudfunctions.googleapis.com%2Fcloud-functions'
        self.logger = logging.getLogger(log_name.format(project))
        if project == "local":
            handler = logging.StreamHandler(sys.stdout)
        else:
            cloud_logging_client = cloudlogging.Client()
            handler = cloud_logging_client.get_default_handler()
        self.logger.addHandler(handler)
        self.resource = cloudlogging.Resource(type="cloud_function", labels={
            "function_name": function_name,
            "region": region
        })
        self.set_log_level(level)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def set_log_level(self, level):
        if level in self.Severity.__members__:
            self.logger.setLevel(self.Severity[level].value)
        else:
            raise print(
                'The Log Level of \'{}\' does not exist.'.format(level))

    def check_level(self, severity):
        return self.Severity[severity] >= self.log_level or self.Severity[severity] == 0

    class Severity(IntEnum):
        CRITICAL = 50
        FATAL = CRITICAL
        ERROR = 40
        WARNING = 30
        WARN = WARNING
        INFO = 20
        DEBUG = 10
        NOTSET = 0


logger = LoggingClient()
