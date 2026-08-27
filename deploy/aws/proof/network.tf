locals {
  runtime_subnets = {
    for index, az in var.availability_zones : az => {
      cidr = var.runtime_subnet_cidrs[index]
      index = index
    }
  }

  database_subnets = {
    for index, az in var.availability_zones : az => {
      cidr = var.database_subnet_cidrs[index]
      index = index
    }
  }
}

resource "aws_vpc" "proof" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = local.name
  }
}

resource "aws_internet_gateway" "proof" {
  vpc_id = aws_vpc.proof.id

  tags = {
    Name = local.name
  }
}

resource "aws_subnet" "runtime" {
  for_each = local.runtime_subnets

  vpc_id                  = aws_vpc.proof.id
  availability_zone       = each.key
  cidr_block              = each.value.cidr
  map_public_ip_on_launch = false

  tags = {
    Name = "${local.name}-runtime-${each.value.index + 1}"
    Tier = "runtime-egress"
  }
}

resource "aws_subnet" "database" {
  for_each = local.database_subnets

  vpc_id                  = aws_vpc.proof.id
  availability_zone       = each.key
  cidr_block              = each.value.cidr
  map_public_ip_on_launch = false

  tags = {
    Name = "${local.name}-database-${each.value.index + 1}"
    Tier = "database-isolated"
  }
}

resource "aws_route_table" "runtime" {
  vpc_id = aws_vpc.proof.id

  tags = {
    Name = "${local.name}-runtime"
  }
}

resource "aws_route" "runtime_ipv4_egress" {
  route_table_id         = aws_route_table.runtime.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.proof.id
}

resource "aws_route_table_association" "runtime" {
  for_each = aws_subnet.runtime

  subnet_id      = each.value.id
  route_table_id = aws_route_table.runtime.id
}

resource "aws_route_table" "database" {
  vpc_id = aws_vpc.proof.id

  tags = {
    Name = "${local.name}-database-isolated"
  }
}

resource "aws_route_table_association" "database" {
  for_each = aws_subnet.database

  subnet_id      = each.value.id
  route_table_id = aws_route_table.database.id
}

resource "aws_db_subnet_group" "runtime" {
  name       = local.name
  subnet_ids = [for az in var.availability_zones : aws_subnet.database[az].id]

  tags = {
    Name = local.name
  }
}

resource "aws_security_group" "runtime" {
  name        = "${local.name}-runtime"
  description = "No ingress; bounded DNS, HTTPS, and PostgreSQL egress for Runtime tasks"
  vpc_id      = aws_vpc.proof.id

  tags = {
    Name = "${local.name}-runtime"
  }
}

resource "aws_security_group" "database" {
  name        = "${local.name}-database"
  description = "PostgreSQL ingress only from the Runtime task security group"
  vpc_id      = aws_vpc.proof.id

  tags = {
    Name = "${local.name}-database"
  }
}

resource "aws_vpc_security_group_egress_rule" "runtime_https" {
  security_group_id = aws_security_group.runtime.id
  description       = "ECR, Secrets Manager, CloudWatch Logs, and approved HTTPS providers"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "runtime_dns_udp" {
  security_group_id = aws_security_group.runtime.id
  description       = "VPC DNS over UDP"
  ip_protocol       = "udp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = var.vpc_cidr
}

resource "aws_vpc_security_group_egress_rule" "runtime_dns_tcp" {
  security_group_id = aws_security_group.runtime.id
  description       = "VPC DNS over TCP fallback"
  ip_protocol       = "tcp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = var.vpc_cidr
}

resource "aws_vpc_security_group_egress_rule" "runtime_postgres" {
  security_group_id            = aws_security_group.runtime.id
  description                  = "PostgreSQL authority"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.database.id
}

resource "aws_vpc_security_group_ingress_rule" "database_postgres" {
  security_group_id            = aws_security_group.database.id
  description                  = "PostgreSQL only from Runtime tasks"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.runtime.id
}
