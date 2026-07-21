import os
from pyspark.sql import SparkSession

def get_spark_session(app_name):
    """
    Creates and returns a SparkSession configured to use Apache Iceberg, 
    Nessie Catalog, MinIO S3 storage, and OpenLineage.
    """
    # Environment variable overrides (useful for running locally or custom profiles)
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    nessie_endpoint = os.environ.get("NESSIE_ENDPOINT", "http://nessie:19120/api/v1")
    s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")
    
    # Packages versions
    spark_version_suffix = "3.5_2.12"
    iceberg_version = "1.5.0"
    nessie_version = "0.77.1"
    hadoop_version = "3.3.4"
    aws_sdk_version = "1.12.262"
    openlineage_version = "1.9.1"
    
    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-{spark_version_suffix}:{iceberg_version}",
        f"org.projectnessie.nessie-integrations:nessie-spark-extensions-{spark_version_suffix}:{nessie_version}",
        f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}",
        f"io.openlineage:openlineage-spark_2.12:{openlineage_version}"
    ]
    
    packages_str = ",".join(packages)
    
    builder = (SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", packages_str)
        # SQL Extensions for Iceberg and Nessie
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        # Configure Nessie Catalog
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", nessie_endpoint)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/")
        # S3 / S3A FileSystem configurations for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Performance tunings for local Docker/Spark standalone
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2"))
        
    # Configure OpenLineage integration
    openlineage_url = os.environ.get("OPENLINEAGE_URL", "")
    if openlineage_url:
        builder = builder \
            .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
            .config("spark.openlineage.transport.type", "http") \
            .config("spark.openlineage.transport.url", openlineage_url) \
            .config("spark.openlineage.namespace", os.environ.get("OPENLINEAGE_NAMESPACE", "cartco-lakehouse")) \
            .config("spark.openlineage.parentJobName", app_name)
            
    spark = builder.getOrCreate()
    return spark
