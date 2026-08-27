terraform {
  # P6C supplies the reviewed bucket/key/region values produced by ../bootstrap.
  # Offline P6A always initializes with -backend=false and cannot contact S3.
  backend "s3" {}
}
