provider "aws" {
  region = var.aws_region
}

# 1. S3 Buckets for Medallion Lakehouse Layers
resource "aws_s3_bucket" "bronze" {
  bucket        = "cartco-lakehouse-bronze-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "silver" {
  bucket        = "cartco-lakehouse-silver-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "gold" {
  bucket        = "cartco-lakehouse-gold-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "warehouse" {
  bucket        = "cartco-lakehouse-warehouse-${var.environment}"
  force_destroy = true
}

# Enable block public access for security best practices
resource "aws_s3_bucket_public_access_block" "block_public" {
  for_each = {
    bronze    = aws_s3_bucket.bronze.id
    silver    = aws_s3_bucket.silver.id
    gold      = aws_s3_bucket.gold.id
    warehouse = aws_s3_bucket.warehouse.id
  }

  bucket = each.value

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 2. RDS PostgreSQL Database for Airflow and Marquez Metadata
resource "aws_db_subnet_group" "db_subnet" {
  name       = "cartco-db-subnet-group-${var.environment}"
  subnet_ids = var.private_subnet_ids

  tags = {
    Name = "CartCo Database Subnet Group"
  }
}

resource "aws_security_group" "db_sg" {
  name        = "cartco-db-sg-${var.environment}"
  description = "Allow inbound PostgreSQL traffic from VPC"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "metadata_db" {
  identifier             = "cartco-metadata-db-${var.environment}"
  allocated_storage      = 20
  max_allocated_storage  = 100
  engine                 = "postgres"
  engine_version         = "14.7"
  instance_class         = "db.t3.medium"
  db_name                = "cartco_metadata"
  username               = "cartco_admin"
  password               = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.db_subnet.name
  vpc_security_group_ids = [aws_security_group.db_sg.id]
  skip_final_snapshot    = true
  multi_az               = var.environment == "prod" ? true : false

  tags = {
    Environment = var.environment
    Project     = "CartCo-Lakehouse"
  }
}

# 3. Managed Streaming for Apache Kafka (MSK) for Live POS Ingest
resource "aws_security_group" "msk_sg" {
  name   = "cartco-msk-sg-${var.environment}"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_cluster" "pos_kafka" {
  cluster_name           = "cartco-pos-kafka-${var.environment}"
  kafka_version          = "3.2.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type = "kafka.t3.small"
    client_subnets = var.private_subnet_ids
    security_groups = [aws_security_group.msk_sg.id]
    storage_info {
      ebs_storage_info {
        volume_size = 50
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  tags = {
    Environment = var.environment
  }
}

# 4. Amazon Managed Workflows for Apache Airflow (MWAA) environment
resource "aws_iam_role" "mwaa_role" {
  name = "cartco-mwaa-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "amazonmq.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "airflow-env.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_policy" {
  name = "cartco-mwaa-policy-${var.environment}"
  role = aws_iam_role.mwaa_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "airflow:PublishMetrics"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*",
          aws_s3_bucket.warehouse.arn,
          "${aws_s3_bucket.warehouse.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:*:*:log-group:airflow-*"
        ]
      }
    ]
  })
}

resource "aws_security_group" "mwaa_sg" {
  name   = "cartco-mwaa-sg-${var.environment}"
  vpc_id = var.vpc_id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_mwaa_environment" "airflow" {
  name               = "cartco-airflow-${var.environment}"
  airflow_version    = "2.6.3"
  environment_class  = "mw1.small"
  execution_role_arn = aws_iam_role.mwaa_role.arn

  network_configuration {
    security_group_ids = [aws_security_group.mwaa_sg.id]
    subnet_ids         = var.private_subnet_ids
  }

  source_bucket_arn = aws_s3_bucket.warehouse.arn
  dag_s3_path       = "dags"

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  tags = {
    Environment = var.environment
  }
}
