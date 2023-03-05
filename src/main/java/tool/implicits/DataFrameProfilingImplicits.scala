package tool.implicits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


/**
 * Those are the functions used for feature engineering
 */
object DataFrameProfilingImplicits {

  implicit class DataFrameProfilingImprovements(df: DataFrame) {

    private val defaultMissingSeq:Seq[String] = Seq("\t", "\\t", "9999-01-01", "null", "-2147483648", "-", "—")

    /**
     * Missing value statistics
     * @param missingFeat string values that should be considered as missing values
     * @return statistics on missing values
     */
    def getMissingStats(missingFeat:Seq[String]=defaultMissingSeq): DataFrame = {
      val cols = df.columns
      cols.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn("missing_" + c, (col(c).isNull or lower(col(c)).isin(missingFeat:_*)).cast(IntegerType))
      }
        .groupBy()
        .avg(cols.map(c => "missing_"+c):_*)
        .na.fill(0)
    }

  }

}
