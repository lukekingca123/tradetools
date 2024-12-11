variable "region" {
  type = string
}

variable "instance_name" {
  description = "Value of the Name tag for the DolphinDB instance"
  type        = string
  default     = "TestDolphinDBServerInstance"
}

variable "ami_id" {
  description = "AMI ID of EC2"
  type        = string
  default     = "ami-0fd48c6031f8700df"
}

variable "instance_type" {
  description = "instance type of the EC2 instance"
  type        = string
  default     = "t2.micro"
}

variable "subnet_id_p1" {
  description = "ID of subnet where DolphinDB should be in"
  type        = string
}

variable "subnet_id_p2" {
  description = "ID of subnet where DolphinDB should be in"
  type        = string
}

variable "subnet_id_p3" {
  description = "ID of subnet where DolphinDB should be in"
  type        = string
}

variable "security_group_ids" {
  type = list(string)
}

variable "key_name" {
  description = "key pair used to ssh the EC2 instance"
  type        = string
}

variable "volume_size" {
  description = "size of the root volume"
  type        = number
  default     = 10
}

variable "volume_type" {
  description = "type of volume"
  type        = string
  default     = "gp3"
}

variable "ddbversion" {
  description = "version of DolphinDB you want to install"
  type        = string
}

variable "az_p1" {
  description = "availability zone of data volume"
  type        = string
}

variable "az_p2" {
  description = "availability zone of data volume"
  type        = string
}

variable "az_p3" {
  description = "availability zone of data volume"
  type        = string
}

variable "datavolume_type" {
  description = "type of data volume"
  type        = string
  default     = "gp3"
}

variable "datavolume_size" {
  description = "size of data volume"
  type        = number
  default     = 100
}

variable "destination" {
  description = "where data volume is mounted"
  type        = string
  default     = "/data"
}

variable "attachment_device_name" {
  description = "device name of attached data volume"
  type        = string
  default     = "/dev/xvdb"
}

variable "key_location" {
  description = "location of EC2 keys"
  type        = string
}

variable "ddb_username" {
  description = "name of DolphinDB user"
  type        = string
  default     = "centos"
}

variable "package_manager_name" {
  description = "name of package manager, like yum or apt-get"
  type        = string
  default     = "yum"
}

variable "replace_dolphindb_lic" {
  description = "Whether to replace dolphindb.lic"
  type        = bool
  default     = true
}

variable "dolphindb_lic_content" {
  type = string
}

variable "cluster_nodes" {
  type = string
}

variable "cluster_cfg" {
  type = string
}

variable "controller_cfg_p1" {
  type = string
}

variable "agent_cfg_p1" {
  type = string
}

variable "controller_cfg_p2" {
  type = string
}

variable "agent_cfg_p2" {
  type = string
}

variable "controller_cfg_p3" {
  type = string
}

variable "agent_cfg_p3" {
  type = string
}




