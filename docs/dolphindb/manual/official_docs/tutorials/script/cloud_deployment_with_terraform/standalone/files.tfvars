dolphindb_cfg_content= <<EOF
localSite=localhost:8848:local8848
mode=single
maxMemSize=16
maxConnections=512
workerNum=4
localExecutors=3
maxBatchJobWorker=4
dataSync=1
OLAPCacheEngineSize=2
TSDBCacheEngineSize=1
newValuePartitionPolicy=add
maxPubConnections=64
subExecutors=4
perfMonitoring=true
lanCluster=0
EOF

dolphindb_lic_content= <<EOF
2042.01.01,8,2,3,Trial Users,0,-1,,1
M61R0ONe5dFVlsrT8TYM5HsSD0o8O03ua7fino0gH3vINnQ5mM8xQElMl+pZNfc+
6ktPaQBGdPdKOjJxgIS9wOnfg4CFiwCAxfr/bAuy6CENElEtHpohkyUq5PBDXFmL
3Zzw4gPcv/jQNCAmDhSwjhjt8HonhAf4dp+jw0dYKZ1aJQU09yVuwTuqE7jzaGhh
6l1c+mnSO39ZEfQPbvHSLGIQcOM77lH05E7ChhXG8tARsW+/vYJMye20z3bCkAjC
VGZdCe7mvYblmlALb/mTvZ5AMS3WZkOsFLbu3yVZDB55hLoczgls7katqeVmpn2n
vbP//yqCmzlMS8M63rdjNQ==
EOF

security_group_ids=[<your security group ids>]