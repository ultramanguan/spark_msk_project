# S3 Gateway Endpoint: no hourly charge, and it keeps the bulk of this project's traffic (Delta table
# reads/writes, checkpoints, the EMR bootstrap wheel, MWAA's DAG sync) off the VPC's NAT Gateway entirely,
# cutting NAT data-processing charges. Attached to every route table in the VPC so it applies regardless
# of which route table the subnets in var.subnet_ids use.
data "aws_route_tables" "all" {
  vpc_id = var.vpc_id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id          = var.vpc_id
  service_name    = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids = data.aws_route_tables.all.ids

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
