package tool.implicits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object DataFrameImplicits {
  implicit class DataFrameImprovements(df: DataFrame) {

    def addFreqCols(cols:Seq[String], nTop:Int): DataFrame = {
      cols.map(col).map(
        c => df.select(c).groupBy(c).count().orderBy(desc("count")).limit(nTop).withColumn("id", monotonically_increasing_id())
      ).reduce((a, b) => a.join(b, "id")).drop("id")
    }
  }

}
