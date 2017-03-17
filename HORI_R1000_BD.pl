#!/usr/bin/perl
###############################################################################
# HIVE PERL应用程序
#
# PURPOSE:   sqoop导入hdfs业务费用明细表(biz_fee_payway)
# AUTHOR:    wdp
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

############################
# hive_common initial start#
############################

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
# hive_common initial end  #
############################

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
  exit 1 if (!defined($hive) || !defined($conn));
  
  # 关闭RDBMS数据库连接
  $conn->{dbh}->disconnect;
  $conn->{dbh} = undef;
  
  # 退出写状态日志
  $hive->writelog(INFO, "RDBMS connection closed");
  $hive->writelog(INFO, "exit($?)");
}

# 前N月:yyyymm
my $vPre3Month = substr(hive_common::addMonths($vDate,-2),0,6);
my $vPre6Month = substr(hive_common::addMonths($vDate,-5),0,6);
my $vPre9Month = substr(hive_common::addMonths($vDate,-8),0,6);
my $vPre12Month = substr(hive_common::addMonths($vDate,-11),0,6);

#$hive->writelog(INFO, "vPre3Month = $vPre3Month");
#$hive->writelog(INFO, "vPre6Month = $vPre6Month");
#$hive->writelog(INFO, "vPre9Month = $vPre9Month");
#$hive->writelog(INFO, "vPre12Month = $vPre12Month");

# 本月月底:yyyymmdd
my $vMonthLastDay = hive_common::getLastDateOfMonth($vDate);

#30天前
my $v30DayBefore = hive_common::addDays($vDate,-30);

my $vRDBMSCon = "jdbc:oracle:thin:\@10.205.17.141:1521:mbdt2";

chomp($vRDBMSCon);

###################################
# 执行主体sql配置 #
# 跟踪类信息必须用[分号]做结束符!!#
###################################


###################################
# extract_to_hdfs 参数说明 #
###################################
# pTargetdir : 目标HDFS路径 
# pConnstr   : RDBMS连接串, e.g.:jdbc:oracle:thin:\@10.205.16.209:1521/eda
# pDBUser    : RDBMS登陆用户名
# pDBPwd     : RDBMS登陆密码
# pQuerystr  : Sqoop导出时查询RDBMS语句
# pSplitCol  : 并行导入时的拆分字段
# pParellel  : 并行数
###################################
sub extract_to_hdfs{
	my ($pTargetdir,$pConnstr,$pDBUser,$pDBPwd,$pQuerystr,$pSplitCol,$pParellel)=@_;

# STEP 1.删除已有HDFS路径
my $vhdfscmd = "hdfs dfs -rm -r -f $pTargetdir";
$hive->writelog(INFO, "EXEC CMD: $vhdfscmd ");
my $result = system($vhdfscmd);
if ($result != 0)
{
	$hive->writelog(INFO, "DELETE HDFS $pTargetdir FAIL!");
	exit 1;
}
else {
	$hive->writelog(INFO, "DELETE HDFS $pTargetdir SUCCESS!");
}


# STEP 2.SQOOP 导入RDBMS表 
my $vSqoopimp = "sqoop import --connect $pConnstr --username $pDBUser --password $pDBPwd ".
            " --query \"$pQuerystr\" --target-dir $pTargetdir ".
            " --fields-terminated-by '\001'  ".
            " --m $pParellel --split-by $pSplitCol  ".
            " --input-null-string '' --input-null-non-string '' --hive-drop-import-delims";
                        
$hive->writelog(INFO, "EXEC CMD: $vSqoopimp ");
my $result = system($vSqoopimp);
if ($result != 0)
{
	$hive->writelog(INFO, "SQOOP TO HDFS $pTargetdir FAIL!");
	exit 1;
}
else {
	$hive->writelog(INFO, "SQOOP TO HDFS $pTargetdir SUCCESS!");
}


# STEP 3.创建HIVE外部表
# RDBMS #
my $hql = <<EOF
   DROP TABLE ORI.BIZ_FEE_PAYWAY;
   CREATE EXTERNAL TABLE ORI.BIZ_FEE_PAYWAY(
     RECID       DECIMAL(16) ,
     BIZFEEDETID DECIMAL(16) ,
     PAYWAY      VARCHAR(1) ,
     PAYFEES     DECIMAL(16,2) ,
     ASSIST      VARCHAR(255),
     CITY        VARCHAR(2),
     SUBPAYWAY   VARCHAR(2)
   ) 
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
   LOCATION '$pTargetdir';

EOF
;

# 提交批量sql的执行;
my $result = $hive->batch_execute_sql($hql);
exit 1 if ($result != 0);

}

#调用Extract过程
my $vTargetDir = "/warehouse/data/ori/biz_fee_payway/$vDate";
my $vEDA = "jdbc:oracle:thin:\@10.205.16.209:1521/eda";
my $vUser = "etl";
my $vPwd  = "8SJglz99";
my $vQuery = "SELECT A.* FROM NODS.TO_BIZ_FEE_PAYWAY_DG A WHERE \\\$CONDITIONS";  ## query必须带 \\\$CONDITIONS  ！！！
my $vSplitCol = "city";
my $vParellel = "20";

#$pTargetdir,$pConnstr,$pDBUser,$pDBPwd,$pQuerystr,$pSplitCol,$pParellel
my $result = extract_to_hdfs($vTargetDir,$vEDA,$vUser,$vPwd,$vQuery,$vSplitCol,$vParellel);
exit 1 if ($result != 0);

exit 0;

