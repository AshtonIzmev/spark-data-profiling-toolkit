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

  test("test_minmaxmean_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema = "true")
    val result = df.getMinMaxMeanStats
    result.show()

    assert(result.columns.length == 19)
  }

  test("test_percentiles_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema = "true")
    val result = df.getPercentileStats
    result.show()

    assert(result.columns.length == 10)
  }

  test("test_frequency_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema = "true")
    val result = df.getFrequencyStats(5)

    assert(result.columns.length == 16)
  }

  test("test_duplicate_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema = "true")
    val result = df.getDuplicateStats

    assert(result.columns.length == 9)
  }

  test("test_date_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema = "true")
    val result = df.getDateTimeStats
    result.show()

    assert(result.columns.length == 5)
  }

  test("test_month_dow_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema = "true")
    val result = df.getMonthDowStats(3)
    result.show()

    assert(result.columns.length == 8)
  }

  test("test_text_length_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema = "true")
    val result = df.getTextLengthStats(Seq("COM1"))
    result.show()

    assert(result.columns.length == 1)
  }

  test("test_word_freq_classic") {
    val df = DataLoading.loadCsv("src/test/resources/isnull.csv", inferSchema = "true")
    val result = df.getTextWordStats(Seq("COM1"), 5)
    result.show()

    assert(result.columns.length == 2)
  }





}
