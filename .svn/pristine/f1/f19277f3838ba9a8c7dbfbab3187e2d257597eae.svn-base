#!/bin/sh


#10 00 * * * /home/hadoop/cmcc/hive/stat_hive_errorcode_m.sh "$(date +'%Y%m')"
bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#shell file path
WORKPATH=`cd $bin/..;pwd`


LOGPATH=${WORKPATH}/logs
if [ ! -d ${LOGPATH} ]; then
  mkdir -p ${LOGPATH}
fi

if [ $# -ne 0  -a  $# -ne 1 ]
then
    echo "Usage: $0 [DATE],like 201610"
    exit 1
fi

DATE=`date -d last-month +%Y%m`

if [ $# -eq 1 ]
then
        DATE=$1       
fi

STAT_TIME=$DATE

HIVE_DB=testcmcc
HIVE_TB=hive_errorcode_m

START_DATE="${STAT_TIME}01"
END_DATE="${STAT_TIME}31"
CREATE_TIME=`date +'%Y%m%d%H%M%S'`
LOGFILE=${LOGPATH}/stat_${HIVE_TB}_${STAT_TIME}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-             CMCC ${HIVE_TB}            -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-       Shell Version 1.0 update(2016-11-10)   -" | tee -a $LOGFILE
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
select r_stattime,${STAT_TIME} as endtime,${CREATE_TIME} as create_time,catalogid,classid,productid,moduleid,pageid,protocol_id,errorcode,sum(cnt) as errorcode_count
from (
select round(r_stattime/100) as r_stattime,catalogid,classid,productid,moduleid,pageid,protocol_id,errorcode,errorcode_count as cnt 
from hive_errorcode_d
where partdate >=${START_DATE} and partdate <=${END_DATE}
) a
group by r_stattime,catalogid,classid,productid,moduleid,pageid,protocol_id,errorcode;" 
echo "*********************************************************************"| tee -a $LOGFILE
echo ${SQL} | tee -a $LOGFILE
echo "*********************************************************************"| tee -a $LOGFILE

hive -e "${SQL}" 1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished the statistics of ${HIVE_TB}." | tee -a $LOGFILE

#-----------------------------------------
# 		SQOOP EXPORT
#-----------------------------------------
DB_URL=jdbc:mysql://192.168.5.64:8066/south_network
DB_USER=root
DB_PASSWD=1qaz2wsx
DB_TABLENAME=REPORT_ERRORCODE_M
DB_COLUMNS="R_STATTIME,ENDTIME,CREATE_TIME,CATALOGID,CLASSID,PRODUCTID,MODULEID,PAGEID,PROTOCOL_ID,ERRORCODE,ERRORCODE_COUNT"
EXPORT_DIR=/user/hive/warehouse/${HIVE_DB}.db/${HIVE_TB}/partdate=${STAT_TIME}

echo `date +"%Y-%m-%d %H:%M:%S"` "exporting to mysql table ${DB_TABLENAME}..." | tee -a $LOGFILE
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.40.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${DB_TABLENAME} --columns ${DB_COLUMNS} --export-dir ${EXPORT_DIR} --fields-terminated-by '|'  1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished export. mysql table:${DB_TABLENAME}." | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "###################end##########################" | tee -a $LOGFILE
exit 0
