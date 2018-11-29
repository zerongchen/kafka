#!/bin/bash

# 1 00 * * * /home/hadoop/cmcc/hive/shell/add_table_partition.sh $(date -d "1 day ago"   +\%Y\%m\%d)

DATE=$1

if [ $# -lt 1 ]; then
        echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage: $0 <DATE>" 
        echo "DATE: for instance, 20161015" 
        exit 1
fi

WORKPATH=/home/hadoop/cmcc/hive
LOGPATH=${WORKPATH}/logs
if [ ! -d ${LOGPATH} ]; then
  mkdir -p ${LOGPATH}
fi

LOGFILE=${WORKPATH}/logs/createpartition_${DATE}.log

DATABASE=cmcc

hive -e "use ${DATABASE};
		alter table hive_serverbuild_flow_min add if not exists partition (partdate=${DATE}) location 'partdate=${DATE}';
		alter table hive_protocol_flow_min add if not exists partition (partdate=${DATE}) location 'partdate=${DATE}';
		alter table hive_destip_flow_min add if not exists partition (partdate=${DATE}) location 'partdate=${DATE}';
		alter table hive_ip_quality_min add if not exists partition (partdate=${DATE}) location 'partdate=${DATE}';
		alter table hive_flow_min add if not exists partition (partdate=${DATE}) location 'partdate=${DATE}';
		alter table original_bill add if not exists partition (BillType = 1,partdate=${DATE}) location 'billtype=1/partdate=${DATE}';
		alter table original_bill add if not exists partition (BillType = 2,partdate=${DATE}) location 'billtype=2/partdate=${DATE}';
		alter table original_bill add if not exists partition (BillType = 3,partdate=${DATE}) location 'billtype=3/partdate=${DATE}';
		alter table original_bill add if not exists partition (BillType = 4,partdate=${DATE}) location 'billtype=4/partdate=${DATE}';
		alter table original_bill add if not exists partition (BillType = 5,partdate=${DATE}) location 'billtype=5/partdate=${DATE}';
		alter table original_bill add if not exists partition (BillType = 6,partdate=${DATE}) location 'billtype=6/partdate=${DATE}';
		alter table original_bill add if not exists partition (BillType = 7,partdate=${DATE}) location 'billtype=7/partdate=${DATE}';" 1>>${LOGFILE} 2>>${LOGFILE}

