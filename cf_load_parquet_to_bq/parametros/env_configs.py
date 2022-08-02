import os
import sys

from parametros.logger import logger


class EnvConfigs:
    def __init__(self):
        self.environment_configs = os.environ
        self.expected_envs = [
            "BQ_DESTINATION_PROJECT"#,
            #"BQ_DESTINATION_DATASET",
            #"BQ_DESTINATION_TABLE"
        ]
        self.validate()

    def validate(self):
        for env in self.expected_envs:
            if env not in os.environ.keys():
                logger.error(f'Please set the environment variable {env}')
                sys.exit(1)

    def get_bq_destination_project(self):
        return self.environment_configs.get("BQ_DESTINATION_PROJECT")

    def get_bq_destination_dataset(self):
        return self.environment_configs.get("BQ_DESTINATION_DATASET")

    def get_bq_destination_table(self):
        return self.environment_configs.get("BQ_DESTINATION_TABLE")
