from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

logging = Logger()


class CustomMetrics:
    def __init__(self, service: str, namespace: str = "Data-Platform-Sourcing"):
        self.metrics = Metrics(namespace=namespace)
        self.service = service

    def add_metric(self, name: str, unit: MetricUnit, value: int):
        
        logging.info(f"Adding metric {name} with value {value}")
        self.metrics.add_metric(name=name, unit=unit, value=value)
        self.metrics.add_metadata(name=name, value=value, service=self.service)
    
    def get_metrics(self):
        return self.metrics

    def publish(self):
        self.metrics.flush_metrics()

    def get_tracer(self):
        tracer = Tracer()
        return tracer
    
    