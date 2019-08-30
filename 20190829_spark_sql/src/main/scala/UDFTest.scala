
/**
 * @author SeraphimLucifar
 * @date 2019-08-29
 */
object UDFTest extends CgApp {
  import session.implicits._
  val df1=sc.parallelize(List(
    (1001,"zhangsan","普通员工",1200,10),
    (1002,"lisi","普通员工",2200,20),
    (1003,"zhanghan","普通员工",1300,20),
    (1004,"xuan","部门经理",4400,30),
    (1005,"wangyan","普通员工",1500,30),
    (1006,"tianqi","部门经理",6600,20),
    (1007,"maba","部门经理",8800,10),
    (1008,"dazhezi","CEO",9900,10)
  )).toDF("empno","ename","job","sal","dno")

df1.createTempView("tb_emp")
session.udf.register("cgfun",(f:String)=>{f+":"+f.size})
session.sql("select cgfun(ename) from tb_emp").show
}
