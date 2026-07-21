variable "aws_region" {
  type        = string
  description = "The AWS Region to deploy resources into"
  default     = "ap-south-1" # Mumbai region for India operations
}

variable "environment" {
  type        = string
  description = "Deploy environment (dev, staging, prod)"
  default     = "dev"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID where the infrastructure will be placed"
  default     = "vpc-0123456789abcdef0"
}

variable "vpc_cidr" {
  type        = string
  description = "CIDR range of the VPC"
  default     = "10.0.0.0/16"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Subnet IDs for DB, MSK, and MWAA placement"
  default     = ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1", "subnet-0123456789abcdef2"]
}

variable "db_password" {
  type        = string
  description = "Master password for PostgreSQL database"
  sensitive   = true
  default     = "SuperSecurePassword123!"
}
