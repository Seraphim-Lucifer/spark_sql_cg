import org.apache.spark.sql.SparkSession

/**
 * @author SeraphimLucifar
 * @date 2019-08-29
 */
class CgApp extends App {
  val session=SparkSession.builder()
    .appName(getClass.getName)
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
  val sc=session.sparkContext
  sc.setLogLevel("WARN")

}
