
import org.apache.spark.sql.SparkSession
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.joda.time.format.DateTimeFormat
import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{LabeledPoint, StandardScaler}
import org.apache.spark.ml.linalg.Vectors



object StreamingApp extends App {

//Todo = This Does not work atm!! change features so that categories are doubles. This may be a change in data collection and not this app.

  val session = SparkSession.builder().appName("ANNDemo")
    .master("local[*]")
    .getOrCreate()

  val lines: RDD[String] = session.sparkContext.textFile("/Users/andydowell/dashboard_for_horses/appoires/horse_and_race/2K_clean_race_and_horse_data_with_age_and_pos_maps.csv")

  val filtered: RDD[String] = lines.filter(
      row => !row.split(",")(2).toString.trim.equals("DOB") &&
      !row.split(",")(9).toString.trim.equals("-") &&
      !row.split(",")(10).toString.trim.equals("-") &&
      !row.split(",")(11).toString.trim.equals("-") &&
      !row.split(",")(12).toString.trim.equals("-") &&
     !row.split(",")(16).toString.trim.equals("-") &&
     convertDobToMonths(row.split(",")(2).toString) != 0L
  )

  filtered.foreach(println)

  import session.implicits._

  val rdd = filtered.map(parseLine).toDS()

//ID	NAME	DOB	AGE OF HORSE	TRAINER	GENDER	SIRE	DAM	OWNER	DATE	POS	RAN	BHA	TYPE	COURSE	DISTANCE	GOING	CLASS	STARTING PRICE	POS_LABEL


  def parseValues(value : String) : Double =
  { value match {
    case "Male" | "France" => 0.0
    case "Female" | "Spain" => 1.0
    case "Germany" => 2.0
    case default => value.toDouble }
  }

  def parseLine(line : String) = {
    println("=========> " + line)
    var fields = line.split(",")

    var vector = fields.map(parseValues)
    LabeledPoint(parseValues(fields(13)), Vectors.dense(vector))
  }

  val scaler = new StandardScaler().setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithMean(true)
    .setWithStd(true)

  val scalerModel = scaler.fit(rdd)

  val data = scalerModel.transform(rdd)

  val splits = data.randomSplit(Array(0.75, 0.25), seed = 1234L)
  val train = splits(0)
  val test = splits(1)


  val layers = Array[Int](9, 5, 5, 5, 5, 5, 2)
  val trainer = new MultilayerPerceptronClassifier()
    .setLayers(layers)
    .setBlockSize(128)
    .setSeed(1234L)
    .setMaxIter(100)

  // train the model
  val model = trainer.fit(train)

  // compute accuracy on the test set

  val result = model.transform(test)
  val predictionAndLabels = result.select("prediction", "label")
  val evaluator = new MulticlassClassificationEvaluator() .setMetricName("accuracy")


  println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels))

  def convertDobToMonths(birthDate:String): Long={

    val dayStr = birthDate.trim().split(" ")(0)
    val year = birthDate.trim().split(" ")(2)
    val month = birthDate.trim().split(" ")(1).toUpperCase
    val day = """\d+""".r findFirstIn(dayStr)
    val today = LocalDate.now


    val format = DateTimeFormat.forPattern("MMM")
    val instance = format.withLocale(Locale.ENGLISH).parseDateTime(month)

    val yearFormat = DateTimeFormat.forPattern("YYYY")
    val yearInstance = yearFormat.withLocale(Locale.ENGLISH).parseDateTime(year)

    val month_number = instance.getMonthOfYear
    val month_text = instance.monthOfYear.getAsText(Locale.ENGLISH)

    val  year_number = yearInstance.getYear

    val birthday = LocalDate.of(year_number,month_number, day.get.toInt)

    ChronoUnit.MONTHS.between(birthday, today)


  }



}