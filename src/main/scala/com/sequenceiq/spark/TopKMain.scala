
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

object TopKMain {
  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)
    val top = args(2)
    val storageLevel = args(3)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val txtFile = sc.textFile(input).map(
      line => line.split(",")
    ).persist(getStorageLevel(storageLevel))


    val result = txtFile.keyBy(arr => arr(0))
     .top(top.toInt)

    sc.parallelize(result, 1).saveAsTextFile(output)

  }
  def getStorageLevel(level: String): StorageLevel = {
    level match {
      case "cache" => StorageLevel.MEMORY_ONLY
      case "mem_and_disk" => StorageLevel.MEMORY_AND_DISK
      case "mem_ser" => StorageLevel.MEMORY_ONLY_SER
      case "mem_and_disk_ser" => StorageLevel.MEMORY_AND_DISK_SER
      case _ => StorageLevel.NONE
    }
  }

}
