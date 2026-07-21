output "bronze_bucket_arn" {
  value       = aws_s3_bucket.bronze.arn
  description = "Bronze bucket ARN"
}

output "silver_bucket_arn" {
  value       = aws_s3_bucket.silver.arn
  description = "Silver bucket ARN"
}

output "gold_bucket_arn" {
  value       = aws_s3_bucket.gold.arn
  description = "Gold bucket ARN"
}

output "warehouse_bucket_arn" {
  value       = aws_s3_bucket.warehouse.arn
  description = "Warehouse bucket ARN"
}

output "database_endpoint" {
  value       = aws_db_instance.metadata_db.endpoint
  description = "Endpoint of the RDS PostgreSQL metadata database instance"
}

output "database_name" {
  value       = aws_db_instance.metadata_db.db_name
  description = "Name of the default database"
}

output "msk_bootstrap_brokers" {
  value       = aws_msk_cluster.pos_kafka.bootstrap_brokers
  description = "Plaintext connection bootstrap brokers string"
}

output "mwaa_webserver_url" {
  value       = aws_mwaa_environment.airflow.webserver_url
  description = "Airflow web interface URL"
}
