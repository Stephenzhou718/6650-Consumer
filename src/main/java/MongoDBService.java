import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

public class MongoDBService {

  private MongoDatabase database;

  public MongoDBService() {
    String connectionString = "mongodb+srv://stephenzhou718:mJhJvq8iW6TBj6gh@cluster-oregon.phv2lgu.mongodb.net/?retryWrites=true&w=majority&appName=cluster-oregon";
//    String connectionString = "mongodb://stephenzhou718:mJhJvq8iW6TBj6gh@docdb-2024-03-29-18-07-46.cluster-cjqsia044z8y.us-west-2.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false";
    String dbName = "6650";

    CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
        CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));

    MongoClientSettings clientSettings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(connectionString))
        .codecRegistry(pojoCodecRegistry)
        .build();

    MongoClient mongoClient = MongoClients.create(clientSettings);
    this.database = mongoClient.getDatabase(dbName);

    // Select the database
    this.database = mongoClient.getDatabase(dbName);
  }

  public void addSkierLog(SkiersLog log) {
    // Get the collection
    String collectionName = "skiers-data";
    MongoCollection<SkiersLog> collection = database.getCollection(collectionName, SkiersLog.class);

    // Insert the document
    collection.insertOne(log);

    System.out.println("Document added to the collection successfully.");
  }

  // Close the MongoDB client connection
  public void closeClient(MongoClient mongoClient) {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }
}
