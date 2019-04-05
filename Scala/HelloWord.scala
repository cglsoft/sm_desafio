object HelloWorld {
  def main(args: Array[String]): Unit = {    
    val textFile = spark.read.textFile("book2.csv") 
    println(textFile)
  }
}

val counts = textFile.flatMap( line => line.split(" "))
  .map( word => (word,1))
  .reduceByKey(_ + _)

https://backtobazics.com/big-data/spark/apache-spark-flatmap-example/



val x = sc.parallelize(List("1A 22 3B",  "1A 2B 3B F1 G1", "1A 22 3B 3B"), 2)

val r1 =  x.flatMap( line => line.split(" "))

r1.collect

val r2 = r1.map( word => (word,1))

r2.collect

val r3 = r2.reduceByKey(_ + _)

r3.collect
