#!/bin/bash

DATE=$1

if [ $# -lt 1 ]; then
        echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage: $0 <DATE>" 
        exit 1
fi

WORKPATH=/home/hadoop/cmcc/hive
LOGPATH=${WORKPATH}/logs
if [ ! -d ${LOGPATH} ]; then
  mkdir -p ${LOGPATH}
fi

LOGFILE=${WORKPATH}/logs/createpartition_${DATE}.log

DATABASE=cmcc
DATABASEPATH=/user/hive/warehouse/${DATABASE}.db

hive -e "use ${DATABASE};
		alter table hive_serverbuild_flow_min drop if exists partition (partdate=${DATE});
		alter table hive_protocol_flow_min drop if exists partition (partdate=${DATE});
		alter table hive_destip_flow_min drop if exists partition (partdate=${DATE});
		alter table hive_ip_quality_min drop if exists partition (partdate=${DATE});
		alter table hive_page_quality_min drop if exists partition (partdate=${DATE});
		alter table hive_flow_min drop if exists partition (partdate=${DATE});" 1>>${LOGFILE} 2>>${LOGFILE}

echo "rm path:${DATABASEPATH}/hive_serverbuild_flow_min/partdate=${DATE}" | tee -a $LOGFILE
hadoop fs -rm -r ${DATABASEPATH}/hive_serverbuild_flow_min/partdate=${DATE} >> ${LOGFILE} 2>&1

echo "rm path:${DATABASEPATH}/hive_protocol_flow_min/partdate=${DATE}" | tee -a $LOGFILE
hadoop fs -rm -r ${DATABASEPATH}/hive_protocol_flow_min/partdate=${DATE} >> ${LOGFILE} 2>&1

echo "rm path:${DATABASEPATH}/hive_destip_flow_min/partdate=${DATE}" | tee -a $LOGFILE
hadoop fs -rm -r ${DATABASEPATH}/hive_destip_flow_min/partdate=${DATE} >> ${LOGFILE} 2>&1

echo "rm path:${DATABASEPATH}/hive_ip_quality_min/partdate=${DATE}" | tee -a $LOGFILE
hadoop fs -rm -r ${DATABASEPATH}/hive_ip_quality_min/partdate=${DATE} >> ${LOGFILE} 2>&1

echo "rm path:${DATABASEPATH}/hive_page_quality_min/partdate=${DATE}" | tee -a $LOGFILE
hadoop fs -rm -r ${DATABASEPATH}/hive_page_quality_min/partdate=${DATE} >> ${LOGFILE} 2>&1

echo "rm path:${DATABASEPATH}/hive_flow_min/partdate=${DATE}" | tee -a $LOGFILE
hadoop fs -rm -r ${DATABASEPATH}/hive_flow_min/partdate=${DATE} >> ${LOGFILE} 2>&1

