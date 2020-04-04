import java.util

import com.microsoft.azure.documentdb.{ConnectionMode, ConnectionPolicy, ConsistencyLevel, Document, DocumentClient, RequestOptions}

import scala.collection.immutable._
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.sql.SparkSession

/**
 * This Scala script does the following:
 * 1. Sets up user permissions and acquires a resource token.
 * 2. Later, uses that resource token to insert a document and then a dataframe into Azure Cosmos DB
 *
 * Before running this script, please set 'url' and 'key' below
 *
 * */


/** 1. Bootstrap the resource token test by setting up permissions.
 * Log in with endpoint and master key this time around.
 * */

// Cosmos DB Endpoint
var url = "your.endpoint";

// This master key is used only to set up permissions, later we will use resource token
var key = "yourmasterkey";

var client = new DocumentClient(url,
  key,
  ConnectionPolicy.GetDefault(),
    ConsistencyLevel.Eventual);


val usertmp = "{ 'id' : 'user2' }";
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

client.close();

/** End of setup */


/** 2. Insert operation using resource token */

/** Insert a document into Azure Cosmos DB (i.e. without Spark) */

var connectionPolicy = ConnectionPolicy.GetDefault();

connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps)

var testClient = new DocumentClient(url,
  resourcetoken,
  connectionPolicy,
  ConsistencyLevel.Eventual);

// Link to collection custdata2 within database
var collectionLink="dbs/idiscm/colls/custdata2";

// Document representing a store item
val doc = new Document();
doc.set("type", "storeItem")
doc.set("name","toothpaste")
doc.set("aisle","A")
doc.set("quantity",3);
doc.set("price",13.33)

testClient.createDocument(collectionLink, doc, null, false)

testClient.close()

/** Insert a dataframe into Azure Cosmos DB using Spark */

// Set up Spark session
var spark = SparkSession
  .builder()
  .appName(name = "Spark resource token")
  .master(master = "local")
  .getOrCreate();

// Config settings for write
val writeConfig = Config(Map(
  "Endpoint" -> url,
  //"Masterkey" -> key,
  "ResourceToken" -> resourcetoken,
  "Database" -> "idiscm",
  "Collection" -> "custdata2"
  //,"Upsert" : True
))

// Needed for dataframe formatting
val spk = spark
import spk.implicits._

var r = new scala.util.Random
var data: Map[String, String] = Map()
data += (("age") -> r.nextInt(100).toString)
val df = Seq(data)

// Write test
df.toDF().write.mode(saveMode = "Overwrite").cosmosDB(writeConfig)

/** End of write test */