# MWAA requires its subnets to route outbound traffic through a NAT Gateway, not directly to an Internet
# Gateway -- it rejects "public" subnets outright. MSK Serverless and EMR have no such requirement, so
# all of this (NAT Gateway, its EIP, the private route table) only gets created when var.enable_mwaa is
# true -- for the default study/interactive path (no MWAA), var.subnet_ids are left exactly as they are,
# public or private, with no NAT Gateway cost at all.
#
# A NAT Gateway itself must live in a public subnet (it needs its own Elastic IP and a direct IGW route),
# so it can't sit inside the private subnets it serves. This turns var.subnet_ids into private subnets by
# giving them a dedicated route table through a NAT Gateway hosted in var.nat_gateway_subnet_id -- an
# existing public subnet left untouched -- without touching the route table any other subnet in the VPC
# uses.

resource "aws_eip" "nat" {
  count = var.enable_mwaa ? 1 : 0

  domain = "vpc"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_nat_gateway" "this" {
  count = var.enable_mwaa ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = var.nat_gateway_subnet_id

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_route_table" "private" {
  count = var.enable_mwaa ? 1 : 0

  vpc_id = var.vpc_id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this[0].id
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-private"
  }
}

# Associating each subnet here moves it off whatever route table it uses today onto this one -- a subnet
# can only belong to one route table at a time, so this is what actually makes var.subnet_ids private.
resource "aws_route_table_association" "private" {
  for_each = var.enable_mwaa ? toset(var.subnet_ids) : toset([])

  subnet_id      = each.value
  route_table_id = aws_route_table.private[0].id
}

# S3 Gateway Endpoint: no hourly charge, and it keeps the bulk of this project's traffic (Delta table
# reads/writes, checkpoints, the EMR bootstrap wheel, MWAA's DAG sync) off the NAT Gateway entirely,
# cutting NAT data-processing charges. Always created (useful regardless of enable_mwaa), attached to
# every route table in the VPC -- the `distinct(concat(...))` folds in aws_route_table.private explicitly
# when it exists, since the data source is read before that route table exists on a first apply and
# wouldn't otherwise pick it up until a second refresh.
data "aws_route_tables" "all" {
  vpc_id = var.vpc_id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id       = var.vpc_id
  service_name = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids = distinct(concat(
    data.aws_route_tables.all.ids,
    var.enable_mwaa ? [aws_route_table.private[0].id] : []
  ))

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
