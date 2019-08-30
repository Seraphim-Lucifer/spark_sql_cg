/**
 * @author SeraphimLucifar
 * @date 2019-08-29
 */
object MysqlTest extends CgApp {
  import session.implicits._
  val df1=session.read.format("jdbc").options(Map(
    "url"->"jdbc:mysql://localhost:3306/d1903",
    "user"->"root",
    "password"->"root",
    "dbtable"->"v_tfa_4y16e"
  )).load()
  df1.show
  println(df1.schema)
  println(df1.head)
//  val df2=sc.parallelize(List((3,"王五","男"),(4,"赵六","女"))).toDF("No","name","sex")


}
