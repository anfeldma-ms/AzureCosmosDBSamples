import com.microsoft.azure.documentdb.ConnectionPolicy
import com.microsoft.azure.documentdb.ConsistencyLevel
import com.microsoft.azure.documentdb.DocumentClient
import com.microsoft.azure.documentdb.RequestOptions
import scala.collection.immutable._
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.sql.SparkSession

/** Bootstrap the resource token test by setting up permissions.
 * Log in with endpoint and master key this time around.
 * */

var url = "https://testdb002.documents.azure.com:443/";
var key = "o2iij6euC58ati2Vjui8CjkZW9AU87y7o84GDe5kez7fo86SfKrpu5lKyOhbhwHQSXYae9e24yCVEMovWHxZzw==";

var client = new DocumentClient(url,
  key,
  ConnectionPolicy.GetDefault(),
    ConsistencyLevel.Eventual);


val usertmp = "{ 'id' : 'user2' }";
//val user1 = client.createUser("dbs/idiscm",new com.microsoft.azure.documentdb.User(usertmp), null)
val user1 = client.readUser("dbs/idiscm/users/user2",null);

val tmpuser = user1.getResource();
val ruser = client.readUser("dbs/idiscm/users/user2",null);
val permission_user = "{  'id': 'a_permission1', 'permissionMode': 'All', 'resource': 'dbs/idiscm/colls/custdata2' }";
val permission = new com.microsoft.azure.documentdb.Permission(permission_user);
val permissionlink = "dbs/idiscm/users/user2/permissions/a_permission1";

client.deletePermission(permissionlink,null)

val requestOptions =  new RequestOptions();
val permtest = client.createPermission("dbs/idiscm/users/user2/",permission,requestOptions);

val jstring = permtest.getResource();
val user1permission = client.readPermission(permissionlink, requestOptions);
val resourcetoken = jstring.getToken();

/** End of bootstrap */


/** Resource token test */

val readConfig2 = Config(Map("Endpoint" -> url,
  "ResourceToken" -> resourcetoken,
  "Database" -> "idiscm",
  "Collection" -> "custdata2",
  "query_custom" -> "SELECT * FROM c"
));

val jstringvalues = permtest.getResponseHeaders();

// Set up Spark session
var spark = SparkSession
  .builder()
  .appName(name = "Spark resource token")
  .master(master = "local")
  .getOrCreate();

// TEST 1 - Read
val coll = spark.sqlContext.read.cosmosDB(readConfig2)

val writeConfig = Config(Map(
  "Endpoint" -> url,
  //"Masterkey" -> key,
  "ResourceToken" -> resourcetoken,
  "Database" -> "idiscm",
  "Collection" -> "custdata2"
  //,"Upsert" : True
))

val spk = spark
import spk.implicits._

var r = new scala.util.Random
var data: Map[String, String] = Map()
data += (("pk" -> "partition key"))
data += (("age") -> r.nextInt(100).toString)
val df = Seq(data)

// TEST 2 -
df.toDF().write.mode(saveMode = "Overwrite").cosmosDB(writeConfig)