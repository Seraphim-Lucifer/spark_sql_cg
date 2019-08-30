/**
 * @author SeraphimLucifar
 * @date 2019-08-29
 */
object HiveTest extends CgApp {
  import session.sql
  sql("select * from cg.tb_emp").show

}
