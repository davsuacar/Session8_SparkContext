import org.apache.spark.{SparkConf, SparkContext}

object ScalaApp extends App {
  val logFile = "src/main/resources/OlympicAthletes.csv"
  val conf = new SparkConf().setMaster("local[2]").setAppName("OlympicMedals")
  val sc = new SparkContext(conf)
  val file = sc.textFile(logFile)

  val olympicMedalRecordsRDD = file.map(x => {
    val arr = x.split(",")
    new OlympicMedalRecords(arr(0), Integer.parseInt(arr(1)), arr(2)
      , Integer.parseInt(arr(3)), arr(5), Integer.parseInt(arr(6)),
      Integer.parseInt(arr(7)), Integer.parseInt(arr(8)))
  }
  )

  // EXERCISE 1

  val exercise1 = olympicMedalRecordsRDD
    .filter(x => x.country == "United States" || x.country == "China")
    .map(x => ((x.year, x.country),x.gold + x.gold + x.bronze))
    .reduceByKey((x, y) => x + y)
    .map(x => ((x._1._1, (x._2, 0))))
    .reduceByKey((x,y) => (x._1, y._1))
    .map(x => (x._1, x._2._1, x._2._2, x._2._1 - x._2._2))


  // EXERCISE 2

  val exercise2 = olympicMedalRecordsRDD
    .map((x => ((x.country, x.year), List(x.gold * 3 + x.gold * 2 + x.bronze * 1))))
    .reduceByKey((x, y) => x ::: y)
    .map(x => (x._1._1, List((x._1._2, x._2.sum))))
    .reduceByKey((x, y) => x ::: y)
    .map(x => (x._1, x._2.sortBy(x => x._2).takeRight(1)(0)._1, x._2.sortBy(x => x._2).takeRight(1)(0)._2))


  // EXERCISE 3

  val exercise3 = olympicMedalRecordsRDD
    .map((x => (x.year, List((x.name, x.gold + x.gold + x.bronze)))))
    .reduceByKey((x, y) => x ::: y)
    .map(x => (x._1, x._2.sortBy(x => x._2).takeRight(3)))


  println("The results for the exercises are the following: ")


  println("The exercice 1: ")
  exercise1.foreach(println)

  println("#################################################################################################################")
  println("The exercice 2: ")
  exercise2.foreach(println)

  println("#################################################################################################################")
  println("The exercice 3: ")
  exercise3.foreach(println)

}

