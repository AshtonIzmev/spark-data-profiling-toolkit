import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.funsuite.AnyFunSuite
import tool.implicits.DataFrameProfilingImplicits.DataFrameProfilingImprovements

class DataFrameProfilingImplicitsTest extends AnyFunSuite {

  val eps = 1e-4f
  implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(eps)

  test("test_isnull_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema="false")
    val result = df.getMissingStats()
    result.show()

    val expectedCols:Seq[String] = Seq("avg(missing_CAT1)", "avg(missing_CAT2)", "avg(missing_COM1)",
      "avg(missing_ID1)", "avg(missing_NUM1)", "avg(missing_NUM2)", "avg(missing_DAT1)", "avg(missing_DAT2)")
    assert(result.columns.foldLeft(true) { (bool, c) => expectedCols.contains(c) })
    assert(result.columns.length == expectedCols.length)
  }

}
