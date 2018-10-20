import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val readme = sc.textFile("README.md")
    print(readme.first)
    val words = readme.flatMap(line => line.split(" "))
    val wordsCountPrep = words.map(word => (word, 1))
    val wordsCount = wordsCountPrep.reduceByKey((acc, newval)=>acc+newval)
    val wordsOrder = wordsCount.sortBy(kvPair => kvPair._2, false)
    print(wordsOrder)
    wordsOrder.saveAsTextFile("/tmp/sparking-wordcount")
  }
}
