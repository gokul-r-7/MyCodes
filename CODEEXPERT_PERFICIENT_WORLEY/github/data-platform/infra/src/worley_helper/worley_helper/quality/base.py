from abc import ABCMeta, abstractmethod
from pyspark.sql import DataFrame
from worley_helper.configuration.config import Configuration


class BaseDataQuality(metaclass=ABCMeta):
    def __init__(self, configuration: Configuration):
        self.configuration = configuration

    @abstractmethod
    def run_quality_checks(self) -> DataFrame:
        """Function that runs the Data Quality checks"""
        pass

    @abstractmethod
    def get_quality_checks_pass(self) -> bool:
        """Function that returns easy to consume status"""
        pass

    @abstractmethod
    def get_quality_checks_data(self) -> bool:
        """Function that returns easy to consume quality data"""
        pass
