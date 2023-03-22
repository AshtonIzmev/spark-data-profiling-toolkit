package tool

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ByteType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, TimestampType}


object Toolkit {

  val NUMERIC_TYPES:Set[DataType] = Set(IntegerType, DoubleType, FloatType, LongType, ByteType, ShortType)
  val DATETIME_TYPES:Set[DataType] = Set(DateType, TimestampType)


  val exampleUdf:UserDefinedFunction = udf { (lib: String) => Option(lib).isDefined && lib.startsWith("amIaLib") }

}
