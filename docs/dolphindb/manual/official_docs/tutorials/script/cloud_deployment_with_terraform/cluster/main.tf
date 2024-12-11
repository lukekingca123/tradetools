terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_instance" "ddb_p1" {
  ami           = var.ami_id
  instance_type = var.instance_type

  subnet_id              = var.subnet_id_p1
  key_name               = var.key_name
  vpc_security_group_ids = var.security_group_ids

  tags = {
    Name = "ddb-p1"
  }

  root_block_device {
    volume_size = var.volume_size
    volume_type = var.volume_type
  }
}

resource "aws_instance" "ddb_p2" {
  ami           = var.ami_id
  instance_type = var.instance_type

  subnet_id              = var.subnet_id_p2
  key_name               = var.key_name
  vpc_security_group_ids = var.security_group_ids

  tags = {
    Name = "ddb-p2"
  }

  root_block_device {
    volume_size = var.volume_size
    volume_type = var.volume_type
  }
}

resource "aws_instance" "ddb_p3" {
  ami           = var.ami_id
  instance_type = var.instance_type

  subnet_id              = var.subnet_id_p3
  key_name               = var.key_name
  vpc_security_group_ids = var.security_group_ids

  tags = {
    Name = "ddb-p3"
  }

  root_block_device {
    volume_size = var.volume_size
    volume_type = var.volume_type
  }
}

resource "aws_ebs_volume" "data_p1" {
  availability_zone = var.az_p1
  size              = var.datavolume_size
  type              = var.datavolume_type
}

resource "aws_ebs_volume" "data_p2" {
  availability_zone = var.az_p2
  size              = var.datavolume_size
  type              = var.datavolume_type
}

resource "aws_ebs_volume" "data_p3" {
  availability_zone = var.az_p3
  size              = var.datavolume_size
  type              = var.datavolume_type
}

resource "aws_volume_attachment" "data_attachment_p1" {
  device_name = var.attachment_device_name
  volume_id   = aws_ebs_volume.data_p1.id
  instance_id = aws_instance.ddb_p1.id
}

resource "aws_volume_attachment" "data_attachment_p2" {
  device_name = var.attachment_device_name
  volume_id   = aws_ebs_volume.data_p2.id
  instance_id = aws_instance.ddb_p2.id
}

resource "aws_volume_attachment" "data_attachment_p3" {
  device_name = var.attachment_device_name
  volume_id   = aws_ebs_volume.data_p3.id
  instance_id = aws_instance.ddb_p3.id
}

resource "null_resource" "mount_data_volume_p1" {
  depends_on = [aws_volume_attachment.data_attachment_p1]
  connection {
    type        = "ssh"
    host        = aws_instance.ddb_p1.public_ip
    user        = var.ddb_username
    private_key = file(var.key_location)
  }

  provisioner "remote-exec" {
    inline = [
      "sudo ${var.package_manager_name} update -y",
      "sudo ${var.package_manager_name} install -y unzip vim wget",
      "sudo mkdir ${var.destination}",
      "sudo mkfs -t ext4 ${aws_volume_attachment.data_attachment_p1.device_name}",
      "sudo mount ${aws_volume_attachment.data_attachment_p1.device_name} ${var.destination}",
      "echo \"${aws_volume_attachment.data_attachment_p1.device_name} ${var.destination}  ext4  defaults,nofail  0  2\"|sudo tee -a /etc/fstab",
      "sudo chown -R ${var.ddb_username}:${var.ddb_username} ${var.destination}",
      "cd ${var.destination}",
      "wget https://cdn.dolphindb.cn/downloads/DolphinDB_Linux64_V${var.ddbversion}.zip",
      "unzip DolphinDB_Linux64_V${var.ddbversion}.zip -d dolphindb",
      "echo \"${var.dolphindb_lic_content}\" > ${var.destination}/dolphindb/server/dolphindb.lic",

      "echo \"${var.controller_cfg_p1}\" > ${var.destination}/dolphindb/server/clusterDemo/config/controller.cfg",
      "echo \"${var.cluster_nodes}\" > ${var.destination}/dolphindb/server/clusterDemo/config/cluster.nodes",
      "echo \"${var.cluster_cfg}\" > ${var.destination}/dolphindb/server/clusterDemo/config/cluster.cfg",
      "echo \"${var.agent_cfg_p1}\" > ${var.destination}/dolphindb/server/clusterDemo/config/agent.cfg",
      "cd dolphindb/server/clusterDemo/config",
      "sed -i 's/P1_PRI_IP/${aws_instance.ddb_p1.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P1_PUB_IP/${aws_instance.ddb_p1.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P2_PRI_IP/${aws_instance.ddb_p2.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P2_PUB_IP/${aws_instance.ddb_p2.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P3_PRI_IP/${aws_instance.ddb_p3.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P3_PUB_IP/${aws_instance.ddb_p3.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",

      "cd ../../",
      "chmod +x dolphindb",
      "cd clusterDemo",
      "sh startController.sh",
      "echo \"Sleeping for 30 seconds...\"",
      "sleep 30",
      "sh startAgent.sh",
      "echo \"executing command: ps aux|grep dolphindb\"",
      "ps aux|grep dolphindb"
    ]
  }
}

resource "null_resource" "mount_data_volume_p2" {
  depends_on = [aws_volume_attachment.data_attachment_p2]

  connection {
    type        = "ssh"
    host        = aws_instance.ddb_p2.public_ip
    user        = var.ddb_username
    private_key = file(var.key_location)
  }

  provisioner "remote-exec" {
    inline = [
      "sudo ${var.package_manager_name} update -y",
      "sudo ${var.package_manager_name} install -y unzip vim wget",
      "sudo mkdir ${var.destination}",
      "sudo mkfs -t ext4 ${aws_volume_attachment.data_attachment_p2.device_name}",
      "sudo mount ${aws_volume_attachment.data_attachment_p2.device_name} ${var.destination}",
      "echo \"${aws_volume_attachment.data_attachment_p2.device_name} ${var.destination}  ext4  defaults,nofail  0  2\"|sudo tee -a /etc/fstab",
      "sudo chown -R ${var.ddb_username}:${var.ddb_username} ${var.destination}",
      "cd ${var.destination}",
      "wget https://cdn.dolphindb.cn/downloads/DolphinDB_Linux64_V${var.ddbversion}.zip",
      "unzip DolphinDB_Linux64_V${var.ddbversion}.zip -d dolphindb",
      "echo \"${var.dolphindb_lic_content}\" > ${var.destination}/dolphindb/server/dolphindb.lic",

      "echo \"${var.controller_cfg_p2}\" > ${var.destination}/dolphindb/server/clusterDemo/config/controller.cfg",
      "echo \"${var.cluster_nodes}\" > ${var.destination}/dolphindb/server/clusterDemo/config/cluster.nodes",
      "echo \"${var.cluster_cfg}\" > ${var.destination}/dolphindb/server/clusterDemo/config/cluster.cfg",
      "echo \"${var.agent_cfg_p2}\" > ${var.destination}/dolphindb/server/clusterDemo/config/agent.cfg",
      "cd dolphindb/server/clusterDemo/config",
      "sed -i 's/P1_PRI_IP/${aws_instance.ddb_p1.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P1_PUB_IP/${aws_instance.ddb_p1.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P2_PRI_IP/${aws_instance.ddb_p2.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P2_PUB_IP/${aws_instance.ddb_p2.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P3_PRI_IP/${aws_instance.ddb_p3.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P3_PUB_IP/${aws_instance.ddb_p3.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",

      "cd ../../",
      "chmod +x dolphindb",
      "cd clusterDemo",
      "sh startController.sh",
      "echo \"Sleeping for 30 seconds...\"",
      "sleep 30",
      "sh startAgent.sh",
      "echo \"executing command: ps aux|grep dolphindb\"",
      "ps aux|grep dolphindb"
    ]
  }
}

resource "null_resource" "mount_data_volume_p3" {
  depends_on = [aws_volume_attachment.data_attachment_p3]

  connection {
    type        = "ssh"
    host        = aws_instance.ddb_p3.public_ip
    user        = var.ddb_username
    private_key = file(var.key_location)
  }

  provisioner "remote-exec" {
    inline = [
      "sudo ${var.package_manager_name} update -y",
      "sudo ${var.package_manager_name} install -y unzip vim wget",
      "sudo mkdir ${var.destination}",
      "sudo mkfs -t ext4 ${aws_volume_attachment.data_attachment_p3.device_name}",
      "sudo mount ${aws_volume_attachment.data_attachment_p3.device_name} ${var.destination}",
      "echo \"${aws_volume_attachment.data_attachment_p3.device_name} ${var.destination}  ext4  defaults,nofail  0  2\"|sudo tee -a /etc/fstab",
      "sudo chown -R ${var.ddb_username}:${var.ddb_username} ${var.destination}",
      "cd ${var.destination}",
      "wget https://cdn.dolphindb.cn/downloads/DolphinDB_Linux64_V${var.ddbversion}.zip",
      "unzip DolphinDB_Linux64_V${var.ddbversion}.zip -d dolphindb",
      "echo \"${var.dolphindb_lic_content}\" > ${var.destination}/dolphindb/server/dolphindb.lic",

      "echo \"${var.controller_cfg_p3}\" > ${var.destination}/dolphindb/server/clusterDemo/config/controller.cfg",
      "echo \"${var.cluster_nodes}\" > ${var.destination}/dolphindb/server/clusterDemo/config/cluster.nodes",
      "echo \"${var.cluster_cfg}\" > ${var.destination}/dolphindb/server/clusterDemo/config/cluster.cfg",
      "echo \"${var.agent_cfg_p3}\" > ${var.destination}/dolphindb/server/clusterDemo/config/agent.cfg",
      "cd dolphindb/server/clusterDemo/config",
      "sed -i 's/P1_PRI_IP/${aws_instance.ddb_p1.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P1_PUB_IP/${aws_instance.ddb_p1.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P2_PRI_IP/${aws_instance.ddb_p2.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P2_PUB_IP/${aws_instance.ddb_p2.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P3_PRI_IP/${aws_instance.ddb_p3.private_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",
      "sed -i 's/P3_PUB_IP/${aws_instance.ddb_p3.public_ip}/g' controller.cfg cluster.nodes cluster.cfg agent.cfg",

      "cd ../../",
      "chmod +x dolphindb",
      "cd clusterDemo",
      "sh startController.sh",
      "echo \"Sleeping for 30 seconds...\"",
      "sleep 30",
      "sh startAgent.sh",
      "echo \"executing command: ps aux|grep dolphindb\"",
      "ps aux|grep dolphindb"
    ]
  }
}
