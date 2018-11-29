#!/bin/sh
set -e
#10 00 * * * /home/hadoop/cmcc/hive/shell/stat_hive_errorcode_h.sh $(date +'%Y%m%d %H')


bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#shell file path
WORKPATH=`cd $bin/..;pwd`

LOGPATH=${WORKPATH}/log
if [ ! -d ${LOGPATH} ]; then
  mkdir -p ${LOGPATH}
fi

if [ $# -ne 0  -a  $# -ne 2 ]
then
    echo "Usage: $0 [DATE] [HOUR],like 20161012 12" 
    exit 1
fi

DATE=`date +%Y%m%d`    
HOUR=`date +%k` 

if [ ${HOUR} -eq 0 ]
then
        DATE=`date -d -1day"$DATE" +%Y%m%d` 
        HOUR=24
    echo "time is 24"
fi
if [ ${HOUR} -eq 1 ]
then
        DATE=`date -d -1day"$DATE" +%Y%m%d`
        HOUR=25
fi
#delay two hour
HOUR=$[ $HOUR - 2 ]

if [ $# -eq 2 ]
then
        DATE=$1     
        HOUR=$2        
fi

STAT_TIME="${DATE} ${HOUR}"

HIVE_DB=cmcc2
HIVE_TB=hive_url_quality_h

START_TIME=${STAT_TIME}:00:00
TS_START_TIME=`date -d"${START_TIME}" +%s`
TS_END_TIME=$[TS_START_TIME+3600]
PART_DATE=`date --date="${START_TIME}" +'%Y%m%d%H'`
CREATE_TIME=`date +'%Y%m%d%H%M%S'`
START_TIME=`date -d @${TS_START_TIME} +'%Y%m%d%H'`
END_TIME=`date -d @${TS_END_TIME} +'%Y%m%d%H'`
TS_START_TIME=${TS_START_TIME}000
TS_END_TIME=${TS_END_TIME}000
LOGFILE=${LOGPATH}/stat_${HIVE_TB}_${START_TIME}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-             CMCC ${HIVE_TB}            -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-       Shell Version 2.0 update(2016-11-10)   -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"` "TS_START_TIME	: ${TS_START_TIME}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "TS_END_TIME	: ${TS_END_TIME}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "START_TIME 	: ${START_TIME}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "END_TIME   	: ${END_TIME}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "PART_DATE 	: ${PART_DATE}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "CREATE_TIME : ${CREATE_TIME}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "WORKPATH  	: ${WORKPATH}"| tee -a $LOGFILE

#-----------------------------------------
# 		HIVE COMPUTE
#-----------------------------------------

echo `date +"%Y-%m-%d %H:%M:%S"` "executing the statistics of ${HIVE_TB}..." | tee -a $LOGFILE

SQL="use ${HIVE_DB};insert overwrite table ${HIVE_TB} partition (partdate = ${DATE},hour=${HOUR}) 
select 
	${START_TIME} as r_starttime,
	${END_TIME} as endtime,
	${CREATE_TIME} as create_time,
	url, 
	sum(uiUpFlow) as flow_up, 
	sum(uiDownFlow) as flow_dn, 
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpSYN>0 then 1 else 0 end) as tcp_connect_count, 
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpSYN>0 and uiTcpRespSucFlag=0 then 1 else 0 end ) as tcp_successconnect_count, 
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpRespSucFlag =0 and uiUpFlow>0 and c3=408 then 1 else 0 end) as http_connecttimeout_count, 
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpRespSucFlag =0 and uiUpFlow>0 and c3>=400 then 1 else 0 end) as http_respfail_count, 
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpRespSucFlag =0 and c4>0  then c4 else 0 end) as http_response_delay, 
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpRespSucFlag =0 and c4>0  then 1 else 0 end) as http_response_delay_count,
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpRespSucFlag =0 then uiTcpAckDelay else 0 end) as tcp_client_delay, 
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpRespSucFlag =0 then uiTcpConDelay else 0 end) as tcp_server_delay, 
	sum(uiUpIPPacket) as tcp_datapackage_up, 
	sum(uiDownIPPacket) as tcp_datapackage_dn, 
	sum(uiUpTcpReOrder) as tcp_retranspackage_up, 
	sum(uiDownTcpReOrder) as tcp_retranspackage_dn,
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpRespSucFlag =0 then (uiTcpConDelay + uiTcpAckDelay) else 0 end) as tcp_shakehands_time,
	sum(case when uiL4protocal =0 and uiSessOverFlag=1 and uiTcpRespSucFlag =0 and uiUpFlow>0 then 1 else 0 end) as http_visit_count,
	content_type
from
(
	select 
		case when instr(c8,'?')=0 then c8 else substr(c8,0,instr(c8,'?')-1) end as url, 
		uiUpFlow,
		uiDownFlow,
		uiL4protocal,
		uiSessOverFlag,
		uiTcpSYN,
		uiTcpRespSucFlag,
		c4,
		c3,
		case when instr(c11,'\\;')=0 then c11 else substr(c11,0,instr(c11,'\\;')-1) end as content_type,
		uiTcpAckDelay,
		uiTcpConDelay,
		uiUpIPPacket,
		uiDownIPPacket,
		uiUpTcpReOrder,
		uiDownTcpReOrder
	from original_bill 
	where partdate=${DATE} and hour=${HOUR} and billtype=103 and uiStartTime>=${TS_START_TIME} and uiStartTime<=${TS_END_TIME} and length(c7)>0 and c7 is not null 
) t1 
group by url,content_type 
order by http_visit_count desc 
limit 10000 ;" 
echo "*********************************************************************"| tee -a $LOGFILE
echo ${SQL} | tee -a $LOGFILE
echo "*********************************************************************"| tee -a $LOGFILE

hive -e "set hive.merge.mapredfiles=true;${SQL}" 1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished the statistics of ${HIVE_TB}." | tee -a $LOGFILE

ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "query failed [${DATE} ${HOUR}],exit $ret !" | tee -a $LOGFILE
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

DB_TABLENAME=REPORT_URL_QUALITY_H
DB_COLUMNS="R_STATTIME,ENDTIME,CREATE_TIME,URL,FLOW_UP,FLOW_DN,TCP_CONNECT_COUNT,TCP_SUCCESSCONNECT_COUNT,HTTP_CONNECTTIMEOUT_COUNT,HTTP_RESPFAIL_COUNT,HTTP_RESPONSE_DELAY,HTTP_RESPONSE_DELAY_COUNT,TCP_CLIENT_DELAY,TCP_SERVER_DELAY,TCP_DATAPACKAGE_UP,TCP_DATAPACKAGE_DN,TCP_RETRANSPACKAGE_UP,TCP_RETRANSPACKAGE_DN,TCP_SHAKEHANDS_TIME,HTTP_VISIT_COUNT,CONTENT_TYPE"
EXPORT_DIR=/user/hive/warehouse/${HIVE_DB}.db/${HIVE_TB}/partdate=${DATE}/hour=${HOUR}

echo `date +"%Y-%m-%d %H:%M:%S"` "exporting to mysql table ${DB_TABLENAME}..." | tee -a $LOGFILE
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.24-bin.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${DB_TABLENAME} --columns ${DB_COLUMNS} --export-dir ${EXPORT_DIR} --fields-terminated-by '|'  1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished export. mysql table:${DB_TABLENAME}." | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "###################end##########################" | tee -a $LOGFILE
exit 0
