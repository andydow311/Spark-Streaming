import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.joda.time.format.DateTimeFormat
import java.util.Locale

import org.apache.spark

object StreamingApp extends App {

  val conf = new SparkConf().setAppName("AppName").setMaster("local[*]")

  val context = new SparkContext(conf)

  val raceRddFromFile = context.textFile("/Users/andydowell/dashboard_for_horses/appoires/horse_and_race/*")

  val rdd = raceRddFromFile.map(f=>{
    f.split(",")
  })

  val courses = new ListBuffer[String]
// ID,NAME,DOB,TRAINER,GENDER,SIRE,DAM,OWNER,DATE,POS,RAN,BHA,TYPE,
// COURSE,DISTANCE,GOING,CLASS,STARTING PRICE
  val mappedRaced = rdd.filter(
                              row => {
                                  !row(2).toString.trim.equals("DOB") &&
                                  !row(10).toString.trim.equals("-") &&
                                  !row(11).toString.trim.equals("-") &&
                                  !row(12).toString.trim.equals("-") &&
                                  !row(16).toString.trim.equals("-") &&
                                  convertDobToMonths(row(2)) != 0L //&&
                                 //  row(13).toString.trim.toLowerCase.equals("bath")
                              }).map(
                                row => (condition(row(9).toString.trim),
                                        (row(0), row(1),  convertDobToMonths(row(2)),row(3), row(4), row(7), row(8), row(10),row(11),row(12),row(13),row(14),row(15),row(16),row(17) )
                                        )
                                ).collect()

  mappedRaced.foreach(
    row => {
      val course = row._2._11.trim.toUpperCase()
      if(!courses.contains(course)) {
        courses.append(course)
      }
    }
  )

 // mappedRaced.foreach(println)

  println("size =======> " + mappedRaced.length)

  def condition(pos:String): String ={

    if(pos.equals("1") || pos.equals("1") || pos.equals("3")){
      "W"
    }else{
      "L"
    }

  }

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