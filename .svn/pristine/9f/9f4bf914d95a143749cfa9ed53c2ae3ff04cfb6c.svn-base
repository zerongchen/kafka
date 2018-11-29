#!/bin/sh


#10 00 * * * /home/hadoop/cmcc/hive/shell/stat_hive_errorcode_h.sh $(date +'%Y%m%d %H')


bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#shell file path
WORKPATH=`cd $bin/..;pwd`

LOGPATH=${WORKPATH}/logs
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
#delay one hour
HOUR=$[ $HOUR - 1 ]

if [ $# -eq 2 ]
then
        DATE=$1     
        HOUR=$2        
fi

STAT_TIME="${DATE} ${HOUR}"

HIVE_DB=testcmcc
HIVE_TB=hive_errorcode_h

START_TIME=${STAT_TIME}:00:00
TS_START_TIME=`date -d"${START_TIME}" +%s`
TS_END_TIME=$[TS_START_TIME+3600]
PART_DATE=`date --date="${START_TIME}" +'%Y%m%d%H'`
CREATE_TIME=`date +'%Y%m%d%H%M%S'`
START_TIME=`date -d @${TS_START_TIME} +'%Y%m%d%H'`
END_TIME=`date -d @${TS_END_TIME} +'%Y%m%d%H'`
LOGFILE=${LOGPATH}/stat_${HIVE_TB}_${START_TIME}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-             CMCC ${HIVE_TB}            -" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-       Shell Version 1.0 update(2016-11-10)   -" | tee -a $LOGFILE
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

SQL="use ${HIVE_DB};insert overwrite table ${HIVE_TB} partition (partdate = ${START_TIME}) 
select
/*+ MAPJOIN(hive_errorcode_info) */ 
${START_TIME} as r_starttime,
${END_TIME} as endtime,
${CREATE_TIME} as create_time,
b.uiAppLevelOne as catalogid,
b.uiAppLevelTwo as classid,
b.uiAppLevelThere as productid,
b.uiAppLevelFour as moduleid,
b.uiAppLevelFive as pageid,
b.uiProtocolType as protocol_id,
b.c3 as errorcode,
count(1) as errorcode_count
 from hive_errorcode_info a join original_bill b on (a.billtype=b.billtype and a.protocol_id=b.uiProtocolType and a.errorcode=b.c3 
 and uiStartTime>=${TS_START_TIME} and uiStartTime<=${TS_END_TIME}) 
where b.partdate=${PART_DATE} and b.billtype=103 
 group by uiAppLevelOne,uiAppLevelTwo,uiAppLevelThere,uiAppLevelFour,uiAppLevelFive,uiProtocolType,c3
union all
select 
/*+ MAPJOIN(hive_errorcode_info) */
${START_TIME} as r_starttime,
${END_TIME} as endtime,
${CREATE_TIME} as create_time,
b.uiAppLevelOne as catalogid,
b.uiAppLevelTwo as classid,
b.uiAppLevelThere as productid,
b.uiAppLevelFour as moduleid,
b.uiAppLevelFive as pageid,
b.uiProtocolType as protocol_id,
b.c1 as errorcode,
count(1) as errorcode_count
 from hive_errorcode_info a join original_bill b on (a.billtype=b.billtype and a.protocol_id=b.uiProtocolType and a.errorcode=b.c1 
 and uiStartTime>=${TS_START_TIME} and uiStartTime<=${TS_END_TIME}) 
where b.partdate=${PART_DATE} and b.billtype=104 
 group by uiAppLevelOne,uiAppLevelTwo,uiAppLevelThere,uiAppLevelFour,uiAppLevelFive,uiProtocolType,c1
union all
select 
/*+ MAPJOIN(hive_errorcode_info) */
${START_TIME} as r_starttime,
${END_TIME} as endtime,
${CREATE_TIME} as create_time,
b.uiAppLevelOne as catalogid,
b.uiAppLevelTwo as classid,
b.uiAppLevelThere as productid,
b.uiAppLevelFour as moduleid,
b.uiAppLevelFive as pageid,
b.uiProtocolType as protocol_id,
b.c2 as errorcode,
count(1) as errorcode_count
 from hive_errorcode_info a join original_bill b on (a.billtype=b.billtype and a.protocol_id=b.uiProtocolType and a.errorcode=b.c2 
 and uiStartTime>=${TS_START_TIME} and uiStartTime<=${TS_END_TIME}) 
where b.partdate=${PART_DATE} and b.billtype=105 
 group by uiAppLevelOne,uiAppLevelTwo,uiAppLevelThere,uiAppLevelFour,uiAppLevelFive,uiProtocolType,c2;" 
echo "*********************************************************************"| tee -a $LOGFILE
echo ${SQL} | tee -a $LOGFILE
echo "*********************************************************************"| tee -a $LOGFILE

hive -e "set hive.merge.mapredfiles=true;${SQL}" 1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished the statistics of ${HIVE_TB}." | tee -a $LOGFILE


#-----------------------------------------
# 		SQOOP EXPORT
#-----------------------------------------
DB_URL=jdbc:mysql://192.168.5.64:8066/south_network
DB_USER=root
DB_PASSWD=1qaz2wsx
DB_TABLENAME=REPORT_ERRORCODE_H
DB_COLUMNS="R_STATTIME,ENDTIME,CREATE_TIME,CATALOGID,CLASSID,PRODUCTID,MODULEID,PAGEID,PROTOCOL_ID,ERRORCODE,ERRORCODE_COUNT"
EXPORT_DIR=/user/hive/warehouse/${HIVE_DB}.db/${HIVE_TB}/partdate=${START_TIME}

echo `date +"%Y-%m-%d %H:%M:%S"` "exporting to mysql table ${DB_TABLENAME}..." | tee -a $LOGFILE
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.40.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${DB_TABLENAME} --columns ${DB_COLUMNS} --export-dir ${EXPORT_DIR} --fields-terminated-by '|'  1>>${LOGFILE} 2>>${LOGFILE}
echo `date +"%Y-%m-%d %H:%M:%S"` "finished export. mysql table:${DB_TABLENAME}." | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "###################end##########################" | tee -a $LOGFILE
exit 0
