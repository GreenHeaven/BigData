#!/usr/bin/perl
###############################################################################
# HIVE PERL应用程序
#
# PURPOSE:   http epg信息(to_rating_http_epg_info) 及同步到RDBMS
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

sub Procedure {
        my ($mydate) = @_;

# 定义SQL语句
my $hql = <<EOF        
  #DEBUG:"STEP 1. 插入${mydate}epg信息";
  INSERT OVERWRITE TABLE ODS.TO_RATING_HTTP_EPG_INFO PARTITION (STADATE = '${mydate}')
  SELECT *
    FROM ORI.HTTP_EPG_INFO
   WHERE FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(STIME), 'yyyy-MM-dd HH:mm:ss'),
                       'yyyyMMdd') = '${mydate}';
EOF
;

# 提交批量sql的执行;
my $result = $hive->batch_execute_sql($hql);
exit 1 if ($result != 0);

# TRUNCATE 中间表
my $result = $conn->execute_sql("TRUNCATE TABLE ODS.SQP_TO_HTTP_EPG_INFO");
if ($result != 0)
{
        $hive->writelog(INFO, "TRUNCATE TABLE ODS.SQP_TO_HTTP_EPG_INFO FAIL!");
        exit 1;
}

# SQOOP 到中间表
my $vSqoop = "sqoop export --connect $vRDBMSCon --username ods --password ods --table SQP_TO_HTTP_EPG_INFO".
                    " --export-dir /user/hive/warehouse/ods.db/to_rating_http_epg_info/stadate=$mydate --fields-terminated-by '\\001'".
                    " --input-null-string '\\\\N' --input-null-non-string '\\\\N' -m 12";
my $result = system($vSqoop);
if ($result != 0)
{
        $hive->writelog(INFO, "SQOOP TO RDBMS ODS.SQP_TO_HTTP_EPG_INFO STADATE = $mydate FAIL!");
        exit 1;
}
else {
        $conn->writelog(INFO, "SQOOP TO RDBMS ODS.SQP_TO_HTTP_EPG_INFO STADATE = $mydate SUCCESS!");
}

my $sql = <<EOF
  #DEBUG: "STEP 1.删除统计日旧数据";
  DELETE ODS.TO_HTTP_EPG_INFO WHERE STADATE = '$mydate';
  COMMIT;
  
  #DEBUG: "STEP 2.插入统计日新数据，ODS.SQP_TO_HTTP_EPG_INFO 为SQOOP导入到RDBMS的统计日数据";
  INSERT INTO ODS.TO_HTTP_EPG_INFO NOLOGGING
   SELECT A.*,'$mydate' STADATE FROM ODS.SQP_TO_HTTP_EPG_INFO A;
  COMMIT;

EOF
;

# 提交批量sql的执行;
my $result = $conn->batch_execute_sql($sql);
if ($result != 0)
{
        $hive->writelog(INFO, "RDBMS EXEC SQL FAIL! :$sql");
        exit 1;
}
else
{
        $hive->writelog(INFO, "RDBMS EXEC SQL SUCCESS! :$sql");
}

return 0;

}

# 统计日数据
my $result = Procedure($vDate);
exit 1 if ($result != 0);

# 系统日期(统计日后一天)数据
my $result = Procedure($vNextDay);
exit 1 if ($result != 0);

exit 0;