#!/bin/sh


#10 00 * * * /home/hadoop/cmcc/hive/shell/stat_hive_biz_flow_d.sh $(date +'%Y%m%d')


bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#shell file path
WORKPATH=`cd $bin/..;pwd`

LOGPATH=${WORKPATH}/log
if [ ! -d ${LOGPATH} ]; then
  mkdir -p ${LOGPATH}
fi

if [ $# -ne 0  -a  $# -ne 1 ]
then
    echo "Usage: $0 [DATE],like 20161012" 
    exit 1
fi

DATE=`date +%Y%m%d`    
DATE=`date -d -1day +%Y%m%d`


if [ $# -eq 1 ]
then
        DATE=$1             
fi

STAT_TIME="${DATE}"

HIVE_DB=cmcc2
HIVE_TB=hive_biz_flow_d

PART_DATE=${STAT_TIME}
CREATE_TIME=`date +'%Y%m%d%H'`
LOGFILE=${LOGPATH}/stat_${HIVE_TB}_${STAT_TIME}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-             CMCC ${HIVE_TB}            -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-       Shell Version 1.0 update(2016-11-10)   -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"` "PART_DATE 	: ${PART_DATE}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "CREATE_TIME : ${CREATE_TIME}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "WORKPATH  	: ${WORKPATH}"| tee -a $LOGFILE

#-----------------------------------------
# 		HIVE COMPUTE
#-----------------------------------------

echo `date +"%Y-%m-%d %H:%M:%S"` "executing the statistics of ${HIVE_TB}..." | tee -a $LOGFILE

SQL="use ${HIVE_DB};insert overwrite table ${HIVE_TB} partition (partdate = ${PART_DATE}) 
select  CAST(${STAT_TIME} AS INT) as R_STATTIME,
        CAST(${STAT_TIME} AS INT) as ENDTIME,
        CAST(${CREATE_TIME} AS INT) as CREATE_TIME,
        BIZ_ID,PARENT_BIZ_ID,SERVERBUILD_ID,SERVERROOM_ID,
        sum(FLOW_UP) as FLOW_UP,
        sum(FLOW_DN) as FLOW_DN,
		SUM(CONNECT_COUNT) as CONNECT_COUNT,
		SUM(SUCCESSCONNECT_COUNT) as SUCCESSCONNECT_COUNT,
		SUM(CONNECTTIMEOUT_COUNT) as CONNECTTIMEOUT_COUNT,
		SUM(RESPFAIL_COUNT) as RESPFAIL_COUNT,
		SUM(RESPONSE_DELAY) as RESPONSE_DELAY,
		SUM(CLIENT_DELAY) as CLIENT_DELAY,
		SUM(SERVER_DELAY) as SERVER_DELAY,
		SUM(DATAPACKAGE_UP) as DATAPACKAGE_UP,
		SUM(DATAPACKAGE_DN) as DATAPACKAGE_DN,
		SUM(RETRANSPACKAGE_UP) as RETRANSPACKAGE_UP,
		SUM(RETRANSPACKAGE_DN) as RETRANSPACKAGE_DN,
		SUM(SESSIONTIME) as SESSIONTIME,
		SUM(MIDPACKAGEFLOW_DN) as MIDPACKAGEFLOW_DN,
		SUM(MIDPACKAGETIME_DN) as MIDPACKAGETIME_DN,
		SUM(BIGPACKAGEFLOW_DN) as BIGPACKAGEFLOW_DN,
		SUM(BIGPACKAGETIME_DN) as BIGPACKAGETIME_DN,
		SUM(TCP_SHAKEHANDS_TIME) as TCP_SHAKEHANDS_TIME,
		SUM(TCP_CONNECT_COUNT) as TCP_CONNECT_COUNT
from hive_biz_flow_h
WHERE partdate=${PART_DATE}
group by BIZ_ID,PARENT_BIZ_ID,SERVERBUILD_ID,SERVERROOM_ID;"

echo "*********************************************************************"| tee -a $LOGFILE
echo ${SQL} | tee -a $LOGFILE
echo "*********************************************************************"| tee -a $LOGFILE

hive -e "set hive.merge.mapredfiles=true;${SQL}" 1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished the statistics of ${HIVE_TB}." | tee -a $LOGFILE

#-----------------------------------------
# 		SQOOP EXPORT
#-----------------------------------------
DBNAME=`awk -F'=' '/DBNAME/{print $2}' ${WORKPATH}/conf/dbconfig`
HOST=`awk -F'=' '/HOST/{print $2}' ${WORKPATH}/conf/dbconfig`
PORT=`awk -F'=' '/PORT/{print $2}' ${WORKPATH}/conf/dbconfig` 
USER=`awk -F'=' '/USER/{print $2}' ${WORKPATH}/conf/dbconfig`
PASSWD=`awk -F'=' '/PASSWD/{print $2}' ${WORKPATH}/conf/dbconfig`

DB_URL=jdbc:mysql://${HOST}:${PORT}/${DBNAME}
DB_USER=${USER}
DB_PASSWD=${PASSWD}

DB_TABLENAME=report_biz_flow_d
DB_COLUMNS="R_STATTIME,ENDTIME,CREATE_TIME,BIZ_ID,PARENT_BIZ_ID,SERVERBUILD_ID,SERVERROOM_ID,FLOW_UP,FLOW_DN,CONNECT_COUNT,SUCCESSCONNECT_COUNT,CONNECTTIMEOUT_COUNT,RESPFAIL_COUNT,RESPONSE_DELAY,CLIENT_DELAY,SERVER_DELAY,DATAPACKAGE_UP,DATAPACKAGE_DN,RETRANSPACKAGE_UP,RETRANSPACKAGE_DN,SESSIONTIME,MIDPACKAGEFLOW_DN,MIDPACKAGETIME_DN,BIGPACKAGEFLOW_DN,BIGPACKAGETIME_DN,TCP_SHAKEHANDS_TIME,TCP_CONNECT_COUNT"
EXPORT_DIR=/user/hive/warehouse/${HIVE_DB}.db/${HIVE_TB}/partdate=${PART_DATE}

echo `date +"%Y-%m-%d %H:%M:%S"` "exporting to mysql table ${DB_TABLENAME}..." | tee -a $LOGFILE
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.24-bin.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${DB_TABLENAME} --columns ${DB_COLUMNS} --export-dir ${EXPORT_DIR} --fields-terminated-by '|'  1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished export. mysql table:${DB_TABLENAME}." | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "###################end##########################" | tee -a $LOGFILE
exit 0
