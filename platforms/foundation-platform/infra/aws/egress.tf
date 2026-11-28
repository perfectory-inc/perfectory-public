# A fixed egress address for the lanes whose provider admits only some AWS address ranges (owner 2026-11-28, contract
# network.fixed_egress): ca.kapanet.or.kr dropped 43.200-43.202 and rt.molit.go.kr everything but 43.201-43.202, so
# a task's random public IP decides by draw. address: one Elastic IP and an egress subnet; gateway: a NAT gateway on
# that address in one public subnet, and the egress subnet's route out through it. With gateway off the egress subnet
# has no route out and the NAT gateway, what costs by the hour, is gone; with address off the address is released.
# The other lanes keep their own public IPs in the public subnets.
locals {
  egress = local.contract.network.fixed_egress
}

resource "aws_eip" "egress" {
  count  = local.egress.address ? 1 : 0
  domain = "vpc"
  tags = {
    Name = "perfectory-platform-fixed-egress"
  }
}

resource "aws_subnet" "egress" {
  count             = local.egress.address ? 1 : 0
  vpc_id            = aws_vpc.platform.id
  availability_zone = local.egress.zone
  cidr_block        = cidrsubnet(var.vpc_cidr, local.contract.network.subnet_newbits, local.egress.subnet_netnum)
  # A task here goes out through the NAT gateway's address, never its own.
  map_public_ip_on_launch = false
  tags = {
    Name = "perfectory-platform-egress-${local.egress.zone}"
    tier = "egress"
  }
}

resource "aws_nat_gateway" "egress" {
  count         = local.egress.address && local.egress.gateway ? 1 : 0
  allocation_id = aws_eip.egress[0].id
  subnet_id     = aws_subnet.public[local.egress.zone].id
  tags = {
    Name = "perfectory-platform-fixed-egress"
  }
  depends_on = [aws_internet_gateway.platform]
}

resource "aws_route_table" "egress" {
  count  = local.egress.address ? 1 : 0
  vpc_id = aws_vpc.platform.id
  dynamic "route" {
    for_each = aws_nat_gateway.egress
    content {
      cidr_block     = "0.0.0.0/0"
      nat_gateway_id = route.value.id
    }
  }
  tags = {
    Name = "perfectory-platform-egress"
  }
}

resource "aws_route_table_association" "egress" {
  count          = local.egress.address ? 1 : 0
  subnet_id      = aws_subnet.egress[0].id
  route_table_id = aws_route_table.egress[0].id
}

# Names and the address only: the runbook's probe reads them.
output "egress_subnet_id" {
  value = try(aws_subnet.egress[0].id, null)
}

output "egress_public_ip" {
  value = try(aws_eip.egress[0].public_ip, null)
}
