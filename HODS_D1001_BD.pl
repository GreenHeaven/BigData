#!/usr/bin/perl
###############################################################################
# HIVE PERL应用程序
#
# PURPOSE:   电视猫频道节目编排表(to_rating_tvm_schedule_info)
# AUTHOR:    admin
# DESCRIPTION:
#
# PARAMETERS:
#    ${vDate}         统计日期(YYYYMMDD)
#    $vBranch         本地网(city) 默认BD (Big Data)
# EXIT STATUS:
#    0                成功
#    其它             失败
###############################################################################

use lib '.';
use strict;
use hive_common;
use dss_common;

# 版本号约束
use constant VERSION => "V01.00.000";

# 入参判断
my $vDate = shift || die "usage: $0 <yyyymmdd>\n";
my $vBranch = shift || "BD";
my $vMonth = substr($vDate,0,6);
my $vYear = substr($vDate,0,4);

# 创建及初始化Hive对象
my $hive = hive_common->new();

# 设置日志文件和终端日志类型(可选,配置文件中配置) 
# 值：DEBUG, INFO, WARN, ERROR, FATAL
$hive->{file_mask} =  eval("DEBUG | INFO | WARNING | ERROR | FATAL");
$hive->{term_mask} =  eval("DEBUG | INFO | WARNING | ERROR | FATAL");

# 日志级别为: EBUG, INFO, WARNING, ERROR, FATAL
$hive->writelog(hive_common::INFO, "VERSION=" . VERSION);
$hive->writelog(hive_common::INFO, "Data_Date=" .${vDate}."  Branch=".$vBranch);

############################
# dss_common initial start #
############################
# 创建数据库连接
my $conn = dss_common->new();

# 执行数据库连接, 默认设置为自动提交事务
$conn->connect_dwdb($vBranch, $vDate);

# 获取当前连接唯一标志
my $ssid = $conn->get_sessionid();

my $de = dss_common::get_parallel($vBranch,"M");
############################
# dss_common initial end   #
############################

############################
# 设置脚本退出执行语句      #
############################

END {
  # 非法入参直接退出
  exit 1 if (!defined($hive));
  
  # 关闭RDBMS数据库连接
  $conn->{dbh}->disconnect;
  $conn->{dbh} = undef;
  
  # 退出写状态日志
  $hive->writelog(INFO, "exit($?)");
}

my $vTomorrow = hive_common::addDays($vDate,1);

# 前N月:yyyymm
my $vPre3Month = substr(hive_common::addMonths($vDate,-2),0,6);
my $vPre6Month = substr(hive_common::addMonths($vDate,-5),0,6);
my $vPre9Month = substr(hive_common::addMonths($vDate,-8),0,6);
my $vPre12Month = substr(hive_common::addMonths($vDate,-11),0,6);

$hive->writelog(INFO, "vPre3Month = $vPre3Month");
$hive->writelog(INFO, "vPre6Month = $vPre6Month");
$hive->writelog(INFO, "vPre9Month = $vPre9Month");
$hive->writelog(INFO, "vPre12Month = $vPre12Month");

# 本月月底:yyyymmdd
my $vMonthLastDay = hive_common::getLastDateOfMonth($vDate);

#一天后
my $vNextDay = hive_common::addDays($vDate,1);

#30天前
my $v30DayBefore = hive_common::addDays($vDate,-30);

my $vRDBMSCon = "jdbc:oracle:thin:\@10.205.17.141:1521:mbdt2";
chomp($vRDBMSCon);

my $vSysdate = $conn->execute_sql("select to_char(sysdate,'yyyymmdd') from dual");

###################################
# 执行主体sql配置 #
# 跟踪类信息必须用[分号]做结束符!!#
###################################

# 定义SQL语句
my $hql = <<EOF        
  #DEBUG:"STEP 1. 插入${vDate}epg信息";
  set hive.exec.compress.output=true;  
  set mapred.output.compress=true;  
  set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;  
  set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec; 
  set hive.exec.dynamic.partition=true;
  set hive.exec.dynamic.partition.mode=nonstrict;
  
  CREATE TABLE IF NOT EXISTS ODS.TO_RATING_TVM_SCHEDULE_INFO(
  TVMCHNLID       VARCHAR(16),
  TVMEPGCODE      VARCHAR(16),
  EVENTID         VARCHAR(16),
  EVENTOP         VARCHAR(8),
  PROGRAMID       VARCHAR(16),
  STIME           VARCHAR(6),
  ETIME           VARCHAR(6),
  PROGRAMNAME     VARCHAR(32),
  EPISODENO       VARCHAR(16),
  RELATEDTEAMS    VARCHAR(32)
  )
  PARTITIONED BY (SCHDATE VARCHAR(8))
  STORED AS ORC;
  
  DROP TABLE IF EXISTS TMP.TMP_D1001_TVMEPG;
  CREATE TABLE TMP.TMP_D1001_TVMEPG AS
  SELECT SCHDATE,
         TVMCHNLID,
         TVMEPGCODE,
         EVENTID,
         EVENTOP,
         PROGRAMID,
         STIME,
         ETIME,
         PROGRAMNAME,
         EPISODENO,
         CONCAT_WS(',', COLLECT_LIST(RELATEDTEAMS)) RELATEDTEAMS
    FROM ORI.TVM_SCHEDULE_INFO
   WHERE SCHDATE BETWEEN '$vDate' AND '$vTomorrow'
   GROUP BY SCHDATE,
            TVMCHNLID,
            TVMEPGCODE,
            EVENTID,
            EVENTOP,
            PROGRAMID,
            STIME,
            ETIME,
            PROGRAMNAME,
            EPISODENO;
  
  DROP TABLE IF EXISTS TMP.TMP_D1001_HTTPEPG;
  CREATE TABLE TMP.TMP_D1001_HTTPEPG AS          
  SELECT A.STADATE SCHDATE,
         CAST(A.CHANNELID AS VARCHAR(16)) TVMCHNLID,
         CAST(A.CHANNELID AS VARCHAR(16)) TVMEPGCODE,
         CAST('' AS VARCHAR(16)) EVENTID,
         CAST('' AS VARCHAR(8)) EVENTOP,
         CAST('' AS VARCHAR(16)) PROGRAMID,
         CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(A.STIME),
                                           'yyyy-MM-dd HH:mm:ss'),
                            'HHmm') AS VARCHAR(6)) STIME,
         CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(A.ETIME),
                                           'yyyy-MM-dd HH:mm:ss'),
                            'HHmm') AS VARCHAR(6)) ETIME,
         CAST(A.PROGRAMNAME AS VARCHAR(32)) PROGRAMNAME,
         CAST('' AS VARCHAR(16)) EPISODENO,
         CAST('' AS STRING) RELATEDTEAMS
    FROM ODS.TO_RATING_HTTP_EPG_INFO A
    LEFT JOIN ODS.TO_RATING_CHANNEL_CFG B
      ON (A.CHANNELID = B.LOGICID)
   WHERE A.STADATE BETWEEN '$vDate' AND '$vTomorrow'
     AND B.EPGSOURCE = 'VODPLAT';            
  
  INSERT OVERWRITE TABLE ODS.TO_RATING_TVM_SCHEDULE_INFO PARTITION(SCHDATE='${vDate}')
  SELECT TVMCHNLID,
         TVMEPGCODE,
         EVENTID,
         EVENTOP,
         PROGRAMID,
         STIME,
         ETIME,
         PROGRAMNAME,
         EPISODENO, 
         RELATEDTEAMS
    FROM TMP.TMP_D1001_TVMEPG WHERE SCHDATE = '$vDate'
  UNION ALL
  SELECT TVMCHNLID,
         TVMEPGCODE,
         EVENTID,
         EVENTOP,
         PROGRAMID,
         STIME,
         ETIME,
         PROGRAMNAME,
         EPISODENO, 
         RELATEDTEAMS
    FROM TMP.TMP_D1001_HTTPEPG WHERE SCHDATE = '$vDate';
  
  INSERT OVERWRITE TABLE ODS.TO_RATING_TVM_SCHEDULE_INFO PARTITION(SCHDATE='${vTomorrow}')
  SELECT TVMCHNLID,
         TVMEPGCODE,
         EVENTID,
         EVENTOP,
         PROGRAMID,
         STIME,
         ETIME,
         PROGRAMNAME,
         EPISODENO, 
         RELATEDTEAMS
    FROM TMP.TMP_D1001_TVMEPG WHERE SCHDATE = '$vTomorrow'
  UNION ALL
  SELECT TVMCHNLID,
         TVMEPGCODE,
         EVENTID,
         EVENTOP,
         PROGRAMID,
         STIME,
         ETIME,
         PROGRAMNAME,
         EPISODENO, 
         RELATEDTEAMS
    FROM TMP.TMP_D1001_HTTPEPG WHERE SCHDATE = '$vTomorrow';
     
  #DEBUG:"创建临时表存储统计日期及第二天的数据用于SQOOP同步";
  DROP TABLE IF EXISTS TMP.TMP_RDBMS_TVMSCH_SYN;
  CREATE TABLE IF NOT EXISTS TMP.TMP_RDBMS_TVMSCH_SYN AS
  SELECT *
    FROM ODS.TO_RATING_TVM_SCHEDULE_INFO
   WHERE SCHDATE BETWEEN '$vDate' AND '$vTomorrow';
  
EOF
;

# 提交批量sql的执行;
my $result = $hive->batch_execute_sql($hql);
exit 1 if ($result != 0);


# TRUNCATE 中间表
my $result = $conn->execute_sql("TRUNCATE TABLE ODS.SQP_TVM_SCHEDULE_INFO");
if ($result != 0)
{
	$hive->writelog(INFO, "TRUNCATE TABLE ODS.SQP_TVM_SCHEDULE_INFO FAIL!");
	exit 1;
}
else
{
	$hive->writelog(INFO, "TRUNCATE TABLE ODS.SQP_TVM_SCHEDULE_INFO SUCCESS!");
}

# SQOOOP 到中间表
my $vSqoop = "sqoop export --connect $vRDBMSCon --username ods --password ods --table SQP_TVM_SCHEDULE_INFO".
                    " --export-dir /user/hive/warehouse/tmp.db/tmp_rdbms_tvmsch_syn --fields-terminated-by '\\001'".
                    " --input-null-string '\\\\N' --input-null-non-string '\\\\N' -m 12";
$hive->writelog("Sqoop CMD: $vSqoop");                    
my $result = system($vSqoop);
if ($result != 0)
{
	$hive->writelog(INFO, "SQOOP TO RDBMS SQP_TVM_SCHEDULE_INFO FAIL!");
	exit 1;
}
else
{
	$hive->writelog(INFO, "SQOOP TO RDBMS SQP_TVM_SCHEDULE_INFO SUCCESS!");
}


my $sql = <<EOF
  #DEBUG: 设置并行度;
  ALTER SESSION ENABLE PARALLEL DML;
  ALTER SESSION FORCE PARALLEL query PARALLEL $de;
  ALTER SESSION FORCE PARALLEL DML PARALLEL $de;
  
  #DEBUG:只更新电视猫的EPG，排除地方台EPG;
  DELETE ODS.TO_TVM_SCHEDULE_INFO
   WHERE SCHDATE BETWEEN '$vDate' AND '$vTomorrow';
  COMMIT;

  INSERT INTO ODS.TO_TVM_SCHEDULE_INFO NOLOGGING
  SELECT SCH.TVMCHNLID,
         SCH.TVMEPGCODE,
         SCH.SCHDATE,
         SCH.EVENTID,
         SCH.EVENTOP,
         SCH.PROGRAMID,
         SCH.STIME,
         SCH.ETIME,
         SCH.PROGRAMNAME,
         SCH.EPISODENO,
         SCHDATE || SCH.STIME || '00' STIME_YMD,
         (CASE
           WHEN STIME > ETIME THEN
            TO_CHAR(TO_DATE(SCHDATE, 'YYYYMMDD') + 1, 'YYYYMMDD') ||
            SCH.ETIME || '00'
           WHEN STIME = ETIME THEN
            SCHDATE || SCH.ETIME || '59'
           ELSE
            SCHDATE || SCH.ETIME || '00'
         END) ETIME_YMD,
         TO_DATE(SCHDATE || SCH.STIME || '00', 'YYYYMMDDHH24MISS') STIME_DATE,
         TO_DATE((CASE
                   WHEN STIME > ETIME THEN
                    TO_CHAR(TO_DATE(SCHDATE, 'YYYYMMDD') + 1, 'YYYYMMDD') ||
                    SCH.ETIME || '00'
                   WHEN STIME = ETIME THEN
                    SCHDATE || SCH.ETIME || '59'
                   ELSE
                    SCHDATE || SCH.ETIME || '00'
                 END),
                 'YYYYMMDDHH24MISS') ETIME_DATE
    FROM ODS.SQP_TVM_SCHEDULE_INFO SCH;
  COMMIT;

EOF
;

# 提交批量sql的执行;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);

exit 0;