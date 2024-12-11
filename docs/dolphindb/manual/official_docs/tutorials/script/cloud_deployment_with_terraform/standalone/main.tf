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

resource "aws_instance" "dolphindb_server" {
  ami           = var.ami_id 
  instance_type = var.instance_type              

  subnet_id = var.subnet_id
  key_name = var.key_name
  vpc_security_group_ids = var.security_group_ids # 替换为您的安全组 ID

  tags = {
    Name = var.instance_name
  }

  root_block_device {
    volume_size = var.volume_size     
    volume_type = var.volume_type     
  }
}

resource "aws_ebs_volume" "data" {
  availability_zone= var.az
  size= var.datavolume_size
  type=var.datavolume_type
}

resource "aws_volume_attachment" "data_attachment" {
  device_name = var.attachment_device_name
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.dolphindb_server.id
}


resource "null_resource" "mount_data_volume" {
  depends_on = [aws_volume_attachment.data_attachment]

  connection {
    type        = "ssh"
    host        = aws_instance.dolphindb_server.public_ip
    user        = var.ddb_username
    private_key = file(var.key_location)
  }

  provisioner "remote-exec" {
    inline = [
        "sudo ${var.package_manager_name} update -y",
        "sudo ${var.package_manager_name} install -y  unzip vim wget",
        "sudo mkdir ${var.destination}",
        "sudo mkfs -t ext4 ${aws_volume_attachment.data_attachment.device_name}",
        "sudo mount ${aws_volume_attachment.data_attachment.device_name} ${var.destination}",
        "echo \"${aws_volume_attachment.data_attachment.device_name} ${var.destination}  ext4  defaults,nofail  0  2\"|sudo tee -a /etc/fstab",
        "sudo chown -R ${var.ddb_username}:${var.ddb_username} ${var.destination}",
        "cd ${var.destination}",
        "wget https://cdn.dolphindb.cn/downloads/DolphinDB_Linux64_V${var.ddbversion}.zip",
        "unzip DolphinDB_Linux64_V${var.ddbversion}.zip -d dolphindb",
        "echo \"${var.dolphindb_cfg_content}\" > ${var.destination}/dolphindb/server/dolphindb.cfg",
        "echo \"${var.dolphindb_lic_content}\" > ${var.destination}/dolphindb/server/dolphindb.lic",      
        "cd dolphindb/server",
        "sh startSingle.sh",
        "echo \"executing command: ps aux|grep dolphindb\"",
        "ps aux|grep dolphindb"             
    ]
  }
}
