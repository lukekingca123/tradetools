output "public_ip_p1" {
  value = aws_instance.ddb_p1.public_ip
}

output "private_ip_p1" {
  value = aws_instance.ddb_p1.private_ip
}

output "public_ip_p2" {
  value = aws_instance.ddb_p2.public_ip
}

output "private_ip_p2" {
  value = aws_instance.ddb_p2.private_ip
}

output "public_ip_p3" {
  value = aws_instance.ddb_p3.public_ip
}

output "private_ip_p3" {
  value = aws_instance.ddb_p3.private_ip
}
