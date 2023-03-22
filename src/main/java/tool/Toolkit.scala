package tool

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType}


object Toolkit {

  val NUMERIC_TYPES:Set[DataType] = Set(IntegerType, DoubleType, FloatType, LongType, ByteType, ShortType)

  val exampleUdf:UserDefinedFunction = udf { (lib: String) => Option(lib).isDefined && lib.startsWith("amIaLib") }

}
