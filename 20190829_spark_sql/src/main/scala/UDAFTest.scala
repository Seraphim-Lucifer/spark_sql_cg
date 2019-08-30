import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * @author SeraphimLucifar
 * @date 2019-08-29
 */
object UDAFTest extends CgApp {
    import session.implicits._
    sc.parallelize(List("zhangsan"->18,"lisi"->22,"wangwuwu"->33)).toDF("name","age").createTempView("tb_users")
  session.udf.register("wpudaf",new CgUADF)
  session.sql("select wpudaf(name) from tb_users").show


}
class CgUADF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType =StructType(
    StructField("name",StringType)::Nil
    )

  override def bufferSchema: StructType = StructType(Array(
   StructField("ch",StringType),StructField("num",IntegerType)
  ))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=""
    buffer(1)=0
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
    var name = input.getString(0)
    var is=new Array[Int](26)
    var index=0
    name.foreach(f=>{
      index=f.toInt-97
      is(index)+=1
    })
    var maxC=""
    var maxCount=0
    for(i <- 0 until is.length){
      if( is(i) >maxCount){
        maxCount=is(i)
        maxC=(i+97).toChar.toString
      }
  }
    buffer(0)=maxC
    buffer(1)=maxCount
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val c1=buffer1.getInt(1)
    val c2=buffer2.getInt(1)
    if(c1<c2){
      buffer1(0)=buffer2(0)
      buffer1(1)=buffer2(1)
    }
  }

  override def evaluate(buffer: Row): Any =
    buffer.getString(0)+":"+buffer.getInt(1)
}
