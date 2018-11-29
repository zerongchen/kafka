#!/bin/sh


#10 00 * * * /home/hadoop/cmcc/hive/shell/stat_hive_destip_flow_m.sh $(date +'%Y%m%d')


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

if [ $# -eq 1 ]
then
        DATE=$1             
fi

STAT_TIME="${DATE}"

HIVE_DB=cmcc2
HIVE_TB=hive_destip_flow_m

PART_DATE=${STAT_TIME}
START_DATE="${STAT_TIME}01"
END_DATE="${STAT_TIME}31"
CREATE_TIME=`date +'%Y%m%d%H'`
LOGFILE=${LOGPATH}/stat_${HIVE_TB}_${STAT_TIME}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-             CMCC ${HIVE_TB}            -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-       Shell Version 1.0 update(2016-11-10)   -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"` "START_DATE  : ${START_DATE}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "END_DATE    : ${END_DATE}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "PART_DATE 	: ${PART_DATE}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "CREATE_TIME : ${CREATE_TIME}"| tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"` "WORKPATH  	: ${WORKPATH}"| tee -a $LOGFILE

#-----------------------------------------
# 		HIVE COMPUTE
#-----------------------------------------

echo `date +"%Y-%m-%d %H:%M:%S"` "executing the statistics of ${HIVE_TB}..." | tee -a $LOGFILE

SQL="use ${HIVE_DB};insert overwrite table ${HIVE_TB} partition (partdate = ${PART_DATE}) 
select  CAST(CAST(${START_DATE} AS INT)/100 AS INT) as R_STATTIME,
        CAST(CAST(${END_DATE} AS INT)/100 AS INT) as ENDTIME,
        CAST(${CREATE_TIME} AS INT) as CREATE_TIME,
        DEST_IP,
        sum(FLOW_UP) as FLOW_UP,
        sum(FLOW_DN) as FLOW_DN
FROM hive_destip_flow_d
WHERE partdate >=${START_DATE} and partdate <=${END_DATE}
group by DEST_IP;"

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

DB_TABLENAME=report_destip_flow_m
DB_COLUMNS="R_STATTIME,ENDTIME,CREATE_TIME,DESTIP,FLOW_UP,FLOW_DN"
EXPORT_DIR=/user/hive/warehouse/${HIVE_DB}.db/${HIVE_TB}/partdate=${PART_DATE}

echo `date +"%Y-%m-%d %H:%M:%S"` "exporting to mysql table ${DB_TABLENAME}..." | tee -a $LOGFILE
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.24-bin.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${DB_TABLENAME} --columns ${DB_COLUMNS} --export-dir ${EXPORT_DIR} --fields-terminated-by '|'  1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished export. mysql table:${DB_TABLENAME}." | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "###################end##########################" | tee -a $LOGFILE
exit 0
