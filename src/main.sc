import java.io.IOException
import java.util
import com.google.gson.Gson
import com.microsoft.azure.documentdb.ConnectionPolicy
import com.microsoft.azure.documentdb.ConsistencyLevel
import com.microsoft.azure.documentdb.Database
import com.microsoft.azure.documentdb.Document
import com.microsoft.azure.documentdb.DocumentClient
import com.microsoft.azure.documentdb.DocumentClientException
import com.microsoft.azure.documentdb.DocumentCollection
import com.microsoft.azure.documentdb.RequestOptions
import com.microsoft.azure.documentdb.User
import com.microsoft.azure.documentdb.Permission
import java.util.UUID

import scala.collection.JavaConverters._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
// Import Necessary Libraries
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.documentdb

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.sql.SparkSession

var url = "https://testdb002.documents.azure.com:443/";
var key = "o2iij6euC58ati2Vjui8CjkZW9AU87y7o84GDe5kez7fo86SfKrpu5lKyOhbhwHQSXYae9e24yCVEMovWHxZzw==";

var client = new DocumentClient(url,
  key,
  ConnectionPolicy.GetDefault(),
    ConsistencyLevel.Eventual);

/* new CosmosClientBuilder()
  .setEndpoint(AccountSettings.HOST)
  .setKey(AccountSettings.MASTER_KEY)
  .setConnectionPolicy(defaultPolicy)
  .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
  .buildClient();*/

val usertmp = "{ 'id' : 'user2' }";
//val user1 = client.createUser("dbs/idiscm",new com.microsoft.azure.documentdb.User(usertmp), null)
val user1 = client.readUser("dbs/idiscm/users/user2",null);

val tmpuser = user1.getResource();
print(tmpuser);

val ruser = client.readUser("dbs/idiscm/users/user2",null);

val permission_user = "{  'id': 'a_permission1', 'permissionMode': 'All', 'resource': 'dbs/idiscm/colls/custdata2' }";

val permission = new com.microsoft.azure.documentdb.Permission(permission_user);

val permissionlink = "dbs/idiscm/users/user2/permissions/a_permission1";

client.deletePermission(permissionlink,null)

val requestOptions =  new RequestOptions();
val permtest = client.createPermission("dbs/idiscm/users/user2/",permission,requestOptions);

print(permtest.getResource().getToken());
val jstring = permtest.getResource();

val user1permission = client.readPermission(permissionlink, requestOptions);

/*
print(requestOptions.getResourceTokenExpirySeconds())
print(requestOptions.getSessionToken())
print(requestOptions.getConsistencyLevel())
print(user1permission.getResponseHeaders())
print(requestOptions.getPartitionKey())

print(user1permission.getResponseHeaders())

print(jstring)

print(jstring.getTimestamp())
print(jstring.getSelfLink())
print(jstring.getResourceId())
print(jstring.getETag())

 */

val resourcetoken = jstring.getToken();

// Configure connection to your collection
val readConfig2 = Config(Map("Endpoint" -> url,
  "ResourceToken" -> resourcetoken,
  "Database" -> "idiscm",
  "Collection" -> "custdata2",
  "query_custom" -> "SELECT * FROM c"
));

/*
print(jstring.getResourceLink())
print(jstring.getResourceLink())
print(jstring.getResourcePartitionKey())

 */
val jstringvalues = permtest.getResponseHeaders();
print(jstringvalues.toString())

var spark = SparkSession
  .builder()
  .appName(name = "Spark resource token")
  .master(master = "local")
  .getOrCreate();

// Create collection connection
val coll = spark.sqlContext.read.cosmosDB(readConfig2)

val writeConfig = Config(Map(
  "Endpoint" -> url,
  //"Masterkey" -> key,
  "ResourceToken" -> resourcetoken,
  "Database" -> "idiscm",
  "Collection" -> "custdata2"
  //,"Upsert" : True
))

import org.apache.spark.sql.Encoders
import java.util
val spk = spark
import spk.implicits._

var r = new scala.util.Random
var data: Map[String, String] = Map()
data += (("pk" -> "partition key"))
data += (("age") -> r.nextInt(100).toString)

val df = Seq(data)

df.toDF().write.mode(saveMode = "Overwrite").cosmosDB(writeConfig)



/*
import org.apache.spark.sql.SaveMode
messages.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)

 */

/*
val user1 = client.readUser("dbs/idiscm/users/user2",null)

val tmpuser = user1.getResource()

print(tmpuser)

val ruser = client.readUser("dbs/idiscm/users/user2",null)

val permission_user = "{  'id': 'a_permission1', 'permissionMode': 'All', 'resource': 'dbs/idiscm/colls/custdata2' }"

val permission = new com.microsoft.azure.documentdb.Permission(permission_user)

val permissionlink = "dbs/idiscm/users/user2/permissions/a_permission1"

client.deletePermission(permissionlink,null)

val requestOptions =  new RequestOptions()
val permtest = client.createPermission("dbs/idiscm/users/user2/",permission,requestOptions)

print(permtest.getResource().getToken())
val jstring = permtest.getResource()

val user1permission = client.readPermission(permissionlink, requestOptions)

print(requestOptions.getResourceTokenExpirySeconds())
print(requestOptions.getSessionToken())
print(requestOptions.getConsistencyLevel())
print(user1permission.getResponseHeaders())
print(requestOptions.getPartitionKey())

print(user1permission.getResponseHeaders())

print(jstring)

print(jstring.getTimestamp())
print(jstring.getSelfLink())
print(jstring.getResourceId())
print(jstring.getETag())

val resourcetoken = jstring.getToken()

// Configure connection to your collection
val readConfig2 = Config(Map("Endpoint" -> url,
  "ResourceToken" -> resourcetoken,
  "Database" -> "idiscm",
  "Collection" -> "custdata2",
  "query_custom" -> "SELECT * FROM c"
))

print(jstring.getResourceLink())
print(jstring.getResourceLink())
print(jstring.getResourcePartitionKey())
val jstringvalues = permtest.getResponseHeaders()
print(jstringvalues.toString())

// Create collection connection
val coll = spark.sqlContext.read.cosmosDB(readConfig2)

display(coll)

val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata"

spark.conf.set(   "fs.azure.account.key.waginput.blob.core.windows.net", "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw==")

val messages = spark.read.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata")

display(messages)

val writeConfig = Config(Map(
  "Endpoint" -> url,
  //"Masterkey" -> key,
  "ResourceToken" -> resourcetoken,
  "Database" -> "idiscm",
  "Collection" -> "custdata2"
  //,"Upsert" : True
))

import org.apache.spark.sql.SaveMode
messages.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)


 */
/*

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


 */