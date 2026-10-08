variable "region" {
  type    = string
  default = "us-west-2"
}

variable "github_repo" {
  description = "owner/name allowed to assume the CI role via OIDC"
  type        = string
  default     = "braaaeeedyn/dockwatch"
}

variable "budget_emails" {
  description = "Who gets the $1 / $5 budget alerts"
  type        = list(string)
}

variable "alert_email" {
  description = "Email subscribed to station alerts (empty = no subscription)"
  type        = string
  default     = ""
}

variable "raw_retention_days" {
  type    = number
  default = 180
}

variable "athena_bytes_cutoff" {
  description = "Per-query scan limit for the dockwatch workgroup (bytes); 1 GB by default"
  type        = number
  default     = 1073741824
}
