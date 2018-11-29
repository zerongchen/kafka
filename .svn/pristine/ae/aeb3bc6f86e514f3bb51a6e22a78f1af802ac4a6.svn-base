#!/bin/sh
set -e
#10 00 * * * /home/hadoop/cmcc/hive/stat_hive_url_quality_d.sh "$(date +'%Y%m%d')"


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
    echo "Usage:$0 [DATE],like 20161012"
    exit 1
fi

DATE=`date -d -1day +%Y%m%d` 

if [ $# -eq 1 ]
then
        DATE=$1       
fi

STAT_TIME=$DATE

HIVE_DB=cmcc2
HIVE_TB=hive_url_quality_d

START_DATE="${STAT_TIME}00"
END_DATE="${STAT_TIME}23"
CREATE_TIME=`date +'%Y%m%d%H%M%S'`
LOGFILE=${LOGPATH}/stat_${HIVE_TB}_${STAT_TIME}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-             CMCC ${HIVE_TB}            -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-       Shell Version 2.0 update(2016-11-10)   -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"` "START_DATE  : ${START_DATE}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "END_DATE    : ${END_DATE}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "CREATE_TIME : ${CREATE_TIME}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "WORKPATH  	: ${WORKPATH}"| tee -a $LOGFILE

#-----------------------------------------
# 		HIVE COMPUTE
#-----------------------------------------


echo `date +"%Y-%m-%d %H:%M:%S"` "executing the statistics of ${HIVE_TB}..." | tee -a $LOGFILE

SQL="use ${HIVE_DB};insert overwrite table ${HIVE_TB} partition (partdate = ${STAT_TIME}) 
select 
	${STAT_TIME} as r_stattime,
	${STAT_TIME} as endtime,
	${CREATE_TIME} as create_time,
	url, 
	sum(flow_up) as flow_up, 
	sum(flow_dn) as flow_dn, 
	sum(tcp_connect_count) as tcp_connect_count, 
	sum(tcp_successconnect_count) as tcp_successconnect_count, 
	sum(http_connecttimeout_count) as http_connecttimeout_count, 
	sum(http_respfail_count) as http_respfail_count, 
	sum(http_response_delay) as http_response_delay, 
	sum(http_response_delay_count) as http_response_delay_count, 
	sum(tcp_client_delay) as tcp_client_delay, 
	sum(tcp_server_delay) as tcp_server_delay, 
	sum(tcp_datapackage_up) as tcp_datapackage_up, 
	sum(tcp_datapackage_dn) as tcp_datapackage_dn, 
	sum(tcp_retranspackage_up) as tcp_retranspackage_up, 
	sum(tcp_retranspackage_dn) as tcp_retranspackage_dn,
	sum(tcp_shakehands_time) as tcp_shakehands_time,
	sum(http_visit_count) as http_visit_count,
	content_type
from hive_url_quality_h
where partdate =${STAT_TIME}
group by url,content_type;" 
echo "*********************************************************************"| tee -a $LOGFILE
echo ${SQL} | tee -a $LOGFILE
echo "*********************************************************************"| tee -a $LOGFILE

hive -e "${SQL}" 1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished the statistics of ${HIVE_TB}." | tee -a $LOGFILE
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "query failed [$STAT_TIME],exit $ret !" | tee -a $LOGFILE
        exit $ret
fi
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

DB_TABLENAME=REPORT_URL_QUALITY_D
DB_COLUMNS="R_STATTIME,ENDTIME,CREATE_TIME,URL,FLOW_UP,FLOW_DN,TCP_CONNECT_COUNT,TCP_SUCCESSCONNECT_COUNT,HTTP_CONNECTTIMEOUT_COUNT,HTTP_RESPFAIL_COUNT,HTTP_RESPONSE_DELAY,HTTP_RESPONSE_DELAY_COUNT,TCP_CLIENT_DELAY,TCP_SERVER_DELAY,TCP_DATAPACKAGE_UP,TCP_DATAPACKAGE_DN,TCP_RETRANSPACKAGE_UP,TCP_RETRANSPACKAGE_DN,TCP_SHAKEHANDS_TIME,HTTP_VISIT_COUNT,CONTENT_TYPE"
EXPORT_DIR=/user/hive/warehouse/${HIVE_DB}.db/${HIVE_TB}/partdate=${STAT_TIME}

echo `date +"%Y-%m-%d %H:%M:%S"` "exporting to mysql table ${DB_TABLENAME}..." | tee -a $LOGFILE
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.24-bin.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${DB_TABLENAME} --columns ${DB_COLUMNS} --export-dir ${EXPORT_DIR} --fields-terminated-by '|'  1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished export. mysql table:${DB_TABLENAME}." | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"` "################end##########################" | tee -a $LOGFILE
exit 0
