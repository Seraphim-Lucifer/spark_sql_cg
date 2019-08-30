import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import java.util.Date

/**
 * @author SeraphimLucifar
 * @date 2019-08-29
 */
object HiveDemo extends CgApp {
  import session.implicits._
  import session.sql
//  HiveUtil.init(session)
//  HiveUtil.loadData(session)
//  sql("select * from spark.tb_emp").show
  HiveUtil.ETLData(session)
  sql("select * from spark.tb_emp_info").show
//  sql("drop table spark.tb_emp_info")
}

object HiveUtil{
  //初始化表格，创建数据库，数据表
  def init(session:SparkSession): Unit ={
  import session.sql
    sql("drop database if exists spark")
    sql("create database if not exists spark")
    sql("use spark")
    sql("drop table if exists tb_emp")
    sql("create table if not exists tb_emp(empno int,ename string,sal double,comm double,job string,deptno int) " +
      "partitioned by (hdate int,day int) " +
      "row format delimited fields terminated by ','")
    //    "location 'hdfs://apache01:9000/user/hive/warehouse/spark.db/' 有时候需要设定location
  }

  //按照分区导入hdfs数据
  def loadData(session:SparkSession)={
    import session.sql
    var date=new Date()
    var dateFormat=new SimpleDateFormat("yyyyMM,dd")
    var datestr=dateFormat.format(date).split(",")
    sql(s"load data inpath 'hdfs://cg01:8020/spark_input/spark.test' into table spark.tb_emp partition (hdate=${datestr(0)},day=${datestr(1)})")

  }

  def ETLData(session:SparkSession): Unit ={
  import session.sql
  val tb_emp_info=sql(s"select empno,ename,sal,job,${System.currentTimeMillis()} timesamp from spark.tb_emp where sal>4000")
  tb_emp_info.write.saveAsTable("spark.tb_emp_info")
  }

}
