variable "prefix" {
  description = "Prefix applied to all resource names."
  type        = string
}

variable "location" {
  description = "Azure region for deployment."
  type        = string
  default     = "eastus"
}

variable "environment" {
  description = "Environment name (e.g. dev, prod)."
  type        = string
  default     = "dev"
}

variable "function_runtime_version" {
  description = "Azure Functions runtime version."
  type        = string
  default     = "~4"
}

variable "timer_schedule" {
  description = "CRON expression for the timer trigger."
  type        = string
  default     = "0 0 * * * *"
}

variable "start_stage" {
  description = "Stage to start from when the timer enqueues the pipeline."
  type        = string
  default     = "all"
}

variable "config_path" {
  description = "Relative path to the pipeline configuration file within the Function App."
  type        = string
  default     = "configs/default.yml"
}

