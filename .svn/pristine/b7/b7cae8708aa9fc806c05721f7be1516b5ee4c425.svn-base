#!/bin/sh


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
HIVE_TB=hive_errorcode_h

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
t2.biz_id,
t2.protocol_id,
t2.errorcode,
count(1) as errorcode_count
from (
	select biz_id,protocol_id,errorcode from (
		select
		/*+ MAPJOIN(hive_errorcode_info) */ 
		b.sbizids as sbizids,
		b.uiProtocolType as protocol_id,
		b.c3 as errorcode
		from hive_errorcode_info a join original_bill b on (a.billtype=b.billtype and a.protocol_id=b.uiProtocolType and a.errorcode=b.c3 
		 and uiStartTime>=${TS_START_TIME} and uiStartTime<=${TS_END_TIME}) 
		where b.partdate=${DATE} and b.hour=${HOUR} and b.billtype=103 
		union all
		select 
		/*+ MAPJOIN(hive_errorcode_info) */
		b.sbizids as sbizids,
		b.uiProtocolType as protocol_id,
		b.c1 as errorcode
		 from hive_errorcode_info a join original_bill b on (a.billtype=b.billtype and a.protocol_id=b.uiProtocolType and a.errorcode=b.c1 
		 and uiStartTime>=${TS_START_TIME} and uiStartTime<=${TS_END_TIME}) 
		where b.partdate=${DATE} and b.hour=${HOUR} and b.billtype=104
		union all
		select 
		/*+ MAPJOIN(hive_errorcode_info) */
		b.sbizids as sbizids,
		b.uiProtocolType as protocol_id,
		b.c2 as errorcode
		 from hive_errorcode_info a join original_bill b on (a.billtype=b.billtype and a.protocol_id=b.uiProtocolType and a.errorcode=b.c2 
		 and uiStartTime>=${TS_START_TIME} and uiStartTime<=${TS_END_TIME}) 
		where b.partdate=${DATE} and b.hour=${HOUR} and b.billtype=105 
	 ) t1 
	  LATERAL VIEW explode(split(t1.sbizids,':')) idTable AS biz_id where length(t1.sbizids)>0
) t2
  group by t2.biz_id,t2.protocol_id,t2.errorcode;" 
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

DB_TABLENAME=REPORT_ERRORCODE_H
DB_COLUMNS="R_STATTIME,ENDTIME,CREATE_TIME,BIZ_ID,PROTOCOL_ID,ERRORCODE,ERRORCODE_COUNT"
EXPORT_DIR=/user/hive/warehouse/${HIVE_DB}.db/${HIVE_TB}/partdate=${DATE}/hour=${HOUR}

echo `date +"%Y-%m-%d %H:%M:%S"` "exporting to mysql table ${DB_TABLENAME}..." | tee -a $LOGFILE
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.24-bin.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${DB_TABLENAME} --columns ${DB_COLUMNS} --export-dir ${EXPORT_DIR} --fields-terminated-by '|'  1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished export. mysql table:${DB_TABLENAME}." | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "###################end##########################" | tee -a $LOGFILE
exit 0
