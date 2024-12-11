# 企业版license
dolphindb_lic_content = <<EOF
EOF

security_group_ids = []

cluster_nodes = <<EOF
localSite,mode
P1_PRI_IP:8800:controller1,controller
P2_PRI_IP:8800:controller2,controller
P3_PRI_IP:8800:controller3,controller
P1_PRI_IP:8801:agent1,agent
P1_PRI_IP:8802:datanode1,datanode
P1_PRI_IP:8803:computenode1,computenode
P2_PRI_IP:8801:agent2,agent
P2_PRI_IP:8802:datanode2,datanode
P2_PRI_IP:8803:computenode2,computenode
P3_PRI_IP:8801:agent3,agent
P3_PRI_IP:8802:datanode3,datanode
P3_PRI_IP:8803:computenode3,computenode
EOF

cluster_cfg = <<EOF
maxMemSize=32
maxConnections=512
workerNum=4
maxBatchJobWorker=4
OLAPCacheEngineSize=2
TSDBCacheEngineSize=2
newValuePartitionPolicy=add
maxPubConnections=64
subExecutors=4
lanCluster=0
enableChunkGranularityConfig=true
datanode1.publicName=P1_PUB_IP
computenode1.publicName=P1_PUB_IP
datanode2.publicName=P2_PUB_IP
computenode2.publicName=P2_PUB_IP
datanode3.publicName=P3_PUB_IP
computenode3.publicName=P3_PUB_IP
EOF

controller_cfg_p1 = <<EOF
mode=controller
localSite=P1_PRI_IP:8800:controller1
dfsReplicationFactor=2
dfsReplicaReliabilityLevel=1
dataSync=1
workerNum=4
maxConnections=512
maxMemSize=8
dfsHAMode=Raft
lanCluster=0
publicName=P1_PUB_IP
EOF

controller_cfg_p2 = <<EOF
mode=controller
localSite=P2_PRI_IP:8800:controller2
dfsReplicationFactor=2
dfsReplicaReliabilityLevel=1
dataSync=1
workerNum=4
maxConnections=512
maxMemSize=8
dfsHAMode=Raft
lanCluster=0
publicName=P2_PUB_IP
EOF

controller_cfg_p3 = <<EOF
mode=controller
localSite=P3_PRI_IP:8800:controller3
dfsReplicationFactor=2
dfsReplicaReliabilityLevel=1
dataSync=1
workerNum=4
maxConnections=512
maxMemSize=8
dfsHAMode=Raft
lanCluster=0
publicName=P3_PUB_IP
EOF

agent_cfg_p1 = <<EOF
mode=agent
localSite=P1_PRI_IP:8801:agent1
controllerSite=P1_PRI_IP:8800:controller1
sites=P1_PRI_IP:8801:agent1:agent,P1_PRI_IP:8800:controller1:controller,P2_PRI_IP:8800:controller2:controller,P3_PRI_IP:8800:controller3:controller
workerNum=4
maxMemSize=4
lanCluster=0
EOF

agent_cfg_p2 = <<EOF
mode=agent
localSite=P2_PRI_IP:8801:agent2
controllerSite=P1_PRI_IP:8800:controller1
sites=P2_PRI_IP:8801:agent2:agent,P1_PRI_IP:8800:controller1:controller,P2_PRI_IP:8800:controller2:controller,P3_PRI_IP:8800:controller3:controller
workerNum=4
maxMemSize=4
lanCluster=0
EOF

agent_cfg_p3 = <<EOF
mode=agent
localSite=P3_PRI_IP:8801:agent3
controllerSite=P1_PRI_IP:8800:controller1
sites=P3_PRI_IP:8801:agent3:agent,P1_PRI_IP:8800:controller1:controller,P2_PRI_IP:8800:controller2:controller,P3_PRI_IP:8800:controller3:controller
workerNum=4
maxMemSize=4
lanCluster=0
EOF

