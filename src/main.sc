import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.sql.SparkSession

object demo {

  import com.sun.org.slf4j.internal.Logger
  import com.sun.org.slf4j.internal.LoggerFactory

  protected var logger = LoggerFactory.getLogger(classOf[Nothing])

  def main(args: Array[String]): Unit = {

    var writeConfig = Config(Map(
      CosmosDBConfig.Endpoint -> "https://testdb002.documents.azure.com:443/",
      CosmosDBConfig.Masterkey -> "o2iij6euC58ati2Vjui8CjkZW9AU87y7o84GDe5kez7fo86SfKrpu5lKyOhbhwHQSXYae9e24yCVEMovWHxZzw==",
      CosmosDBConfig.Database -> "SparkDB",
      CosmosDBConfig.Collection -> "SparkColl"
    ))

      var spark = SparkSession
        .builder()
        .appName(name = "Spark connector sample")
        .master(master = "local")
        .getOrCreate()

    logger.warn("Hello\n\n")

    val spk = spark
    import spk.implicits._

    var r = new scala.util.Random
    var data: Map[String, String] = Map()
    data += (("pk" -> "partition key"))
    data += (("age") -> r.nextInt(100).toString)

    val df = Seq(data)
    df.toDF().write.mode( saveMode = "Overwrite").cosmosDB(writeConfig)

  }
}

print("Pre-main")
demo.main(new Array[String](0))
print("Post-main")
