module "centralized-observability" {
  count = var.enable_centralized_observability ? 1 : 0
  source = "./centralized_monitoring_module"
  environment = var.environment
}