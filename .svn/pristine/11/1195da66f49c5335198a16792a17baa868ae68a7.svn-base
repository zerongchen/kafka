CREATE DATABASE IF NOT EXISTS cmcc 
location '/user/hive/warehouse/cmcc.db/';

use cmcc;

CREATE EXTERNAL TABLE hive_serverbuild_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  province_id bigint, 
  carrier_id int, 
  catalogid int, 
  classid int, 
  productid int, 
  serverbuild_id int, 
  serverroom_id int, 
  flow_up bigint, 
  flow_dn bigint, 
  connect_count bigint, 
  successconnect_count bigint, 
  connecttimeout_count bigint, 
  respfail_count bigint, 
  response_delay bigint, 
  client_delay bigint, 
  server_delay bigint, 
  datapackage_up bigint, 
  datapackage_dn bigint, 
  retranspackage_up bigint, 
  retranspackage_dn bigint, 
  sessiontime bigint, 
  midpackageflow_dn bigint, 
  midpackagetime_dn bigint, 
  bigpackageflow_dn bigint, 
  bigpackagetime_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_serverbuild_flow_min';

CREATE EXTERNAL TABLE hive_protocol_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  catalogid int, 
  classid int, 
  productid int,
  protoflag int, 
  protocol_id int, 
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_protocol_flow_min';


CREATE EXTERNAL TABLE hive_destip_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  destip string, 
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_destip_flow_min';


CREATE EXTERNAL TABLE hive_ip_quality_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
protoflag int, 
  protocol_id int, 
destip string, 
  catalogid int, 
  classid int, 
  productid int, 
  flow_up bigint, 
  flow_dn bigint, 
  connect_count bigint, 
  successconnect_count bigint, 
  connecttimeout_count bigint, 
  respfail_count bigint, 
  response_delay bigint, 
  client_delay bigint, 
  server_delay bigint, 
  datapackage_up bigint, 
  datapackage_dn bigint, 
  retranspackage_up bigint, 
  retranspackage_dn bigint, 
  sessiontime bigint, 
  midpackageflow_dn bigint, 
  midpackagetime_dn bigint, 
  bigpackageflow_dn bigint, 
  bigpackagetime_dn bigint,
  page_view bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_ip_quality_min';


CREATE EXTERNAL TABLE hive_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  province_id bigint, 
  carrier_id int, 
  catalogid int, 
  classid int, 
  productid int, 
moduleid int, 
pageid int, 
resource_mark int, 
serverbuild_id int, 
  serverroom_id int, 
  flow_up bigint, 
  flow_dn bigint, 
  connect_count bigint, 
  successconnect_count bigint, 
  connecttimeout_count bigint, 
  respfail_count bigint, 
  response_delay bigint, 
  client_delay bigint, 
  server_delay bigint, 
  datapackage_up bigint, 
  datapackage_dn bigint, 
  retranspackage_up bigint, 
  retranspackage_dn bigint, 
  sessiontime bigint, 
  midpackageflow_dn bigint, 
  midpackagetime_dn bigint, 
  bigpackageflow_dn bigint, 
  bigpackagetime_dn bigint,
  page_view bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_flow_min';

CREATE EXTERNAL TABLE original_bill(
uiBillType int,
uiFloorID int,
uiRoomID int,
uiVisitFlag int,
uiStatisType int,
uiCountryCode int,
uiProvinceCode int,
uiCityCode int,
uiAppLevelOne int,
uiAppLevelTwo int,
uiAppLevelThere int,
uiAppLevelFour int,
uiAppLevelFive int,
sTopDomainName   string,
xDRLength int,
uiCity int,
uiInterfaceType int,
uixDRID string,
uiStartTime bigint,
uiEndTime bigint,
uiProtocolType int,
uiAppType int,
uiAppSubType int,
uiAppContent int,
uiAppStatus int,
sUSERIPv4 string,
sUSERIPv6 string,
uiUserPort int,
uiL4protocal int,
sServerIPv4 string,
sServerIPv6 string,
uiServerPort int,
uiUpFlow int,
uiDownFlow int,
uiUpIPPacket int,
uiDownIPPacket int,
uiUpTcpOutOrder int,
uiDownTcpOutOrder int,
uiUpTcpReOrder int,
uiDownTcpReOrder int,
uiTcpConDelay int,
uiTcpAckDelay int,
uiUpIpFragPacket int,
uiDownIpFragPacket int,
uiTcpConSuccDelay int,
uiConResSuccDelay int,
uiTcpWindow int,
uiTcpMSS int,
uiTcpSYN int,
uiTcpRespSucFlag int,
uiSessOverFlag int,
c1 string,
c2 string,
c3 string,
c4 string,
c5 string,
c6 string,
c7 string,
c8 string,
c9 string,
c10 string,
c11 string,
c12 string,
c13 string,
c14 int,
c15 int,
c16 int,
c17 int,
c18 int,
c19 int,
c20 int,
c21 int,
c22 int,
c23 int,
c24 int)
PARTITIONED BY ( 
  BillType int,partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/original_bill';
  
CREATE EXTERNAL TABLE hive_errorcode_h(
r_stattime int, 
endtime int, 
create_time bigint, 
catalogid int,
classid int,
productid int,
moduleid int,
pageid int,
protocol_id int,
errorcode string,
errorcode_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_errorcode_h';
  
CREATE EXTERNAL TABLE hive_errorcode_d(
r_stattime int, 
endtime int, 
create_time bigint, 
catalogid int,
classid int,
productid int,
moduleid int,
pageid int,
protocol_id int,
errorcode string,
errorcode_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_errorcode_d';
  
CREATE EXTERNAL TABLE hive_errorcode_m(
r_stattime int, 
endtime int, 
create_time bigint, 
catalogid int,
classid int,
productid int,
moduleid int,
pageid int,
protocol_id int,
errorcode string,
errorcode_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_errorcode_m';
  
CREATE EXTERNAL TABLE hive_errorcode_info(
billtype int,
protocol_name string,
protocol_id int,
errorcode string,
errorcode_desc string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_errorcode_info';
 
CREATE EXTERNAL TABLE hive_page_quality_min(
r_stattime int, 
endtime int, 
create_time int, 
productid int, 
moduleid int, 
pageid int, 
flow_up bigint, 
flow_dn bigint, 
connect_count bigint, 
successconnect_count bigint, 
connecttimeout_count bigint, 
respfail_count bigint, 
response_delay bigint, 
client_delay bigint, 
server_delay bigint, 
datapackage_up bigint, 
datapackage_dn bigint, 
retranspackage_up bigint, 
retranspackage_dn bigint, 
page_view bigint, 
sessiontime bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_page_quality_min';
  
CREATE EXTERNAL TABLE hive_page_quality_h(
r_stattime int, 
endtime int, 
create_time int, 
productid int, 
moduleid int, 
pageid int, 
flow_up bigint, 
flow_dn bigint, 
connect_count bigint, 
successconnect_count bigint, 
connecttimeout_count bigint, 
respfail_count bigint, 
response_delay bigint, 
client_delay bigint, 
server_delay bigint, 
datapackage_up bigint, 
datapackage_dn bigint, 
retranspackage_up bigint, 
retranspackage_dn bigint, 
page_view bigint, 
sessiontime bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_page_quality_h';
  
CREATE EXTERNAL TABLE hive_page_quality_d(
r_stattime int, 
endtime int, 
create_time int, 
productid int, 
moduleid int, 
pageid int, 
flow_up bigint, 
flow_dn bigint, 
connect_count bigint, 
successconnect_count bigint, 
connecttimeout_count bigint, 
respfail_count bigint, 
response_delay bigint, 
client_delay bigint, 
server_delay bigint, 
datapackage_up bigint, 
datapackage_dn bigint, 
retranspackage_up bigint, 
retranspackage_dn bigint, 
page_view bigint, 
sessiontime bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_page_quality_d';
  
CREATE EXTERNAL TABLE hive_page_quality_m(
r_stattime int, 
endtime int, 
create_time int, 
productid int, 
moduleid int, 
pageid int, 
flow_up bigint, 
flow_dn bigint, 
connect_count bigint, 
successconnect_count bigint, 
connecttimeout_count bigint, 
respfail_count bigint, 
response_delay bigint, 
client_delay bigint, 
server_delay bigint, 
datapackage_up bigint, 
datapackage_dn bigint, 
retranspackage_up bigint, 
retranspackage_dn bigint, 
page_view bigint, 
sessiontime bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_page_quality_m';
  
CREATE EXTERNAL TABLE hive_biz_flow_min(
r_stattime int, 
endtime int, 
create_time int, 
biz_id int, 
parent_biz_id int, 
serverbuild_id int, 
serverroom_id int, 
flow_up bigint, 
flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_biz_flow_min';
  
CREATE EXTERNAL TABLE hive_biz_flow_h(
r_stattime int, 
endtime int, 
create_time int, 
biz_id int, 
parent_biz_id int, 
serverbuild_id int, 
serverroom_id int, 
flow_up bigint, 
flow_dn bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_biz_flow_h';
  
CREATE EXTERNAL TABLE hive_biz_flow_d(
r_stattime int, 
endtime int, 
create_time int, 
biz_id int, 
parent_biz_id int, 
serverbuild_id int, 
serverroom_id int, 
flow_up bigint, 
flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_biz_flow_d';
  
CREATE EXTERNAL TABLE hive_biz_flow_m(
r_stattime int, 
endtime int, 
create_time int, 
biz_id int, 
parent_biz_id int, 
serverbuild_id int, 
serverroom_id int, 
flow_up bigint, 
flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/cmcc.db/hive_biz_flow_m';