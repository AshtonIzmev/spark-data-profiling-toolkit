package tool.implicits

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import tool.Toolkit


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

    /**
     * MinMaxMean statistics
     *
     * @return statistics on min, max and mean values
     */
    def getMinMaxMeanStats(): DataFrame = {
      val numericCols = df.schema.filter(p => Toolkit.NUMERIC_TYPES.contains(p.dataType)).map(_.name)
      val x = numericCols.map(c => Seq(min(c), max(c), avg(c), stddev(c), skewness(c), kurtosis(c)))
        .reduce((a, b) => a ++ b)
      println(x)
      df.select(numericCols.map(col):_*)
        .groupBy()
        .agg(count("*"), x:_*)
    }

    /**
     * Percentile statistics
     *
     * @return median, quarter and 75% percentile
     */
    def getPercentileStats(): DataFrame = {
      val numericCols = df.schema.filter(p => Toolkit.NUMERIC_TYPES.contains(p.dataType)).map(_.name)
      df.select(numericCols.map(col): _*)
        .groupBy()
        .agg(count("*"),
          numericCols.map(c => percentile_approx(col(c), lit(0.25), lit(100))) ++
            numericCols.map(c => percentile_approx(col(c), lit(0.5), lit(100))) ++
            numericCols.map(c => percentile_approx(col(c), lit(0.75), lit(100))): _*)
    }




  }

}
