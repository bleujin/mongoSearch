package net.ion.nsearcher.indexer.storage.mongo;

import java.io.IOException;

import org.apache.lucene.store.Directory;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class StorageFac {

	public static Directory createToMongo(String hostAddress, String dbName, String prefix) throws MongoException, IOException{
		return createToMongo(new Mongo(hostAddress), dbName, prefix) ;
	}


	public static Directory createToMongo(Mongo mongo, String dbName, String prefix) throws MongoException, IOException{
		return new DistributedDirectory(new MongoDirectory(mongo, dbName, prefix));
	}

	
	public static Directory createToMongoSharding(String hostAddress, String dbName, String prefix) throws MongoException, IOException{
		Mongo mongo = new Mongo(hostAddress) ;
		MongoDirectory mdir = new MongoDirectory(mongo, dbName, prefix, true, false) ;
		return new DistributedDirectory(mdir);
	}


	public static Directory testRecreateToMongo(String hostAddress, String dbName, String prefix) throws MongoException, IOException{
		Mongo mongo = new Mongo(hostAddress) ;
		mongo.dropDatabase(dbName) ;
		MongoDirectory mdir = new MongoDirectory(mongo, dbName, prefix) ;
		return new DistributedDirectory(mdir);
	}
}
