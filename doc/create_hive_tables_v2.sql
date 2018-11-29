CREATE DATABASE IF NOT EXISTS cmcc2 
location '/user/hive/warehouse/cmcc2.db/';

use cmcc2;

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
sBizIds string,
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_errorcode_h(
r_stattime int, 
endtime int, 
create_time bigint, 
biz_id int,
protocol_id int,
errorcode string,
errorcode_count bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_errorcode_d(
r_stattime int, 
endtime int, 
create_time bigint, 
biz_id int,
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_errorcode_m(
r_stattime int, 
endtime int, 
create_time bigint, 
biz_id int,
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
CREATE EXTERNAL TABLE hive_serverbuild_quality_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_serverbuild_quality_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_serverbuild_quality_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_serverbuild_quality_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


  
CREATE EXTERNAL TABLE hive_resource_biz_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_resource_biz_flow_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_resource_biz_flow_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_resource_biz_flow_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
  
  
CREATE EXTERNAL TABLE hive_province_quality_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_province_quality_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_province_quality_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_province_quality_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
  
CREATE EXTERNAL TABLE hive_biz_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  parent_biz_id int,
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_biz_flow_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  parent_biz_id int,
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_biz_flow_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  parent_biz_id int,
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_biz_flow_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  parent_biz_id int,
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
  
  
CREATE EXTERNAL TABLE hive_carrier_province_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  province_id bigint, 
  carrier_id int, 
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_carrier_province_flow_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  province_id bigint, 
  carrier_id int, 
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_carrier_province_flow_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
  province_id bigint, 
  carrier_id int, 
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_carrier_province_flow_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
  province_id bigint, 
  carrier_id int, 
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


  
CREATE EXTERNAL TABLE hive_carrier_province_quality_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint,
  visit_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_carrier_province_quality_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint,
  visit_count bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_carrier_province_quality_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint,
  visit_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_carrier_province_quality_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  province_id bigint, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint,
  visit_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


  
  
CREATE EXTERNAL TABLE hive_carrier_quality_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int,
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint,
  visit_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_carrier_quality_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_carrier_quality_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_carrier_quality_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
  biz_id int, 
  carrier_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
  
  
  
CREATE EXTERNAL TABLE hive_ip_quality_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  dest_ip string,
  biz_id int,
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_ip_quality_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  dest_ip string,
  biz_id int, 
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_ip_quality_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
  dest_ip string,
  biz_id int,
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_ip_quality_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
  dest_ip string,
  biz_id int,
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
  tcp_shakehands_time bigint,
  tcp_connect_count bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

 
CREATE EXTERNAL TABLE hive_destip_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
  dest_ip string,
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_destip_flow_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  dest_ip string,
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_destip_flow_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
  dest_ip string,
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_destip_flow_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
  dest_ip string,
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
  
   
CREATE EXTERNAL TABLE hive_protocol_flow_min(
  r_stattime int, 
  endtime int, 
  create_time int, 
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_protocol_flow_h(
  r_stattime int, 
  endtime int, 
  create_time int, 
  protoflag int, 
  protocol_id int,
  flow_up bigint, 
  flow_dn bigint)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_protocol_flow_d(
  r_stattime int, 
  endtime int, 
  create_time int, 
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
CREATE EXTERNAL TABLE hive_protocol_flow_m(
  r_stattime int, 
  endtime int, 
  create_time int, 
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE EXTERNAL TABLE hive_url_quality_h(
  r_stattime int, 
  endtime int, 
  create_time bigint, 
  url string, 
  flow_up bigint, 
  flow_dn bigint, 
  tcp_connect_count bigint, 
  tcp_successconnect_count bigint, 
  http_connecttimeout_count bigint, 
  http_respfail_count bigint, 
  http_response_delay bigint, 
  http_response_delay_count bigint,
  tcp_client_delay bigint, 
  tcp_server_delay bigint, 
  tcp_datapackage_up bigint, 
  tcp_datapackage_dn bigint, 
  tcp_retranspackage_up bigint, 
  tcp_retranspackage_dn bigint,
  tcp_shakehands_time bigint,
  http_visit_count bigint,
  content_type string)
PARTITIONED BY ( 
  partdate int,hour string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE EXTERNAL TABLE hive_url_quality_d(
  r_stattime int, 
  endtime int, 
  create_time bigint, 
  url string, 
  flow_up bigint, 
  flow_dn bigint, 
  tcp_connect_count bigint, 
  tcp_successconnect_count bigint, 
  http_connecttimeout_count bigint, 
  http_respfail_count bigint, 
  http_response_delay bigint, 
  http_response_delay_count bigint,
  tcp_client_delay bigint, 
  tcp_server_delay bigint, 
  tcp_datapackage_up bigint, 
  tcp_datapackage_dn bigint, 
  tcp_retranspackage_up bigint, 
  tcp_retranspackage_dn bigint,
  tcp_shakehands_time bigint,
  http_visit_count bigint,
  content_type string)
PARTITIONED BY ( 
  partdate int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
