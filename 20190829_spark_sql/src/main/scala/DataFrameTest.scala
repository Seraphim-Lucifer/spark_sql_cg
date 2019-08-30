
/**
 * @author SeraphimLucifar
 * @date 2019-08-29
 */
object DataFrameTest extends CgApp {
  import session.implicits._

  val df1=sc.parallelize(List(
    (1001,"张三",18,2000,"普通员工",10),
    (1002,"李四",20,2500,"普通员工",10),
    (1003,"王五",22,4100,"普通员工",20),
    (1004,"赵六",19,2300,"普通员工",20),
    (1005,"田七",30,4300,"普通员工",30),
    (1006,"强强",28,3200,"普通员工",30),
    (1007,"小刘",31,5000,"部门经理",20),
    (1008,"小李",28,6000,"部门经理",30),
    (1009,"老王",28,8000,"老板",10)
  )).toDF("empno","ename","age","sal","job","deptno")

  val df2=sc.parallelize(List(
    (10,"总裁办","北京"),
    (20,"财务部","深圳"),
    (30,"行政部","山西"),
    (40,"消防部","火星")
  )).toDF("deptno","dname","location")

  val df3=sc.parallelize((1000,3000,"C")::(3001,5000,"B")::(5001,7999,"A")::(8000,Int.MaxValue,"SSR")::Nil)
    .toDF("sallow","salhigh","salgrade")

  //  df1.join(df2,df1("deptno")===df2("deptno")&&df1("age")>20).show()
  //  df1.createTempView("emp")
  //  df2.createTempView("dept")
  //  session.sql("select * from emp e join dept d on e.deptno=d.deptno and e.age>20").show
  //  df1.groupBy("deptno").agg("sal"->"sum","sal"->"avg").show
  //  df1.cube("deptno","job").count().show
  //df1.dropDuplicates("job").show
  //  df2.union(df3).show
  //  df1.join(df2,"deptno").join(df3,df1("sal")>=df3("sallow")&&df1("sal")<=df3("salhigh"))
  //    .select("ename","sal","job","dname","salgrade").orderBy("salgrade").show
  df1.join(df2,"deptno"::Nil,"right").show
  df1.join(df2,df1("deptno")===df2("deptno"),"left").show

}
