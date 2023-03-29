locals {
  env          = "Dev"
  acl          = "private"
  content_type = "application/x-directory"
}

# create bucket for raw and clean data (transition stage)
resource "aws_s3_bucket" "transition_storage" {
  bucket = "transition-storage-dev"
  tags = {
    Name        = "transition stage"
    Environment = local.env
  }
}

resource "aws_s3_bucket_acl" "transition_storage_acl" {
  bucket = aws_s3_bucket.transition_storage.id
  acl    = local.acl
}

resource "aws_s3_bucket_public_access_block" "transition_storage_access_block" {
  bucket = aws_s3_bucket.transition_storage.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create path for the different transition stage
resource "aws_s3_object" "transition_storage_raw_data" {
  bucket       = aws_s3_bucket.transition_storage.id
  key          = "raw-data/"
  content_type = local.content_type
}
resource "aws_s3_object" "transition_storage_clean_data" {
  bucket       = aws_s3_bucket.transition_storage.id
  key          = "clean-data/"
  content_type = local.content_type
}

# -------------------------------------------------------------------
# Create bucket for final dataset (data lake)
resource "aws_s3_bucket" "data_lake" {
  bucket = "fires-data-lake-dev"
  tags = {
    Name        = "data lake final stage"
    Environment = local.env
  }
}

resource "aws_s3_bucket_acl" "data_lake_acl" {
  bucket = aws_s3_bucket.data_lake.id
  acl    = local.acl
}

resource "aws_s3_bucket_public_access_block" "data_lake_access_block" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create path for the data lake
resource "aws_s3_object" "data_lake_final_dataset" {
  bucket       = aws_s3_bucket.data_lake.id
  key          = "fires-dataset-dev/"
  content_type = local.content_type
}