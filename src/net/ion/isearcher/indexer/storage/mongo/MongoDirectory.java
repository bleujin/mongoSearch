package net.ion.isearcher.indexer.storage.mongo;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.ion.framework.util.Closure;
import net.ion.framework.util.CollectionUtil;
import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

public class MongoDirectory implements NosqlDirectory {
	
	private static final String $INC = "$inc";
	private static final String _ID = "_id";
	private static final String FILE_COUNTER = "fileCounter";
	private static final String COUNTER = "counter";
	
	public static final String BLOCK_SIZE = "blockSize";
	public static final String BLOCK_NUMBER = "blockNumber";
	public static final String LAST_MODIFIED = "lastModified";
	public static final String LENGTH = "length";
	public static final String FILE_NAME = "fileName";
	public static final String FILE_NUMBER = "fileNumber";
	public static final String COMPRESSED = "compressed";
	
	public static String BYTES = "bytes";
	
	public static final String FILES_SUFFIX = ".files";
	public static final String BLOCKS_SUFFIX = ".blocks";
	public static final String COUNTER_SUFFIX = ".counter";
	
	private final Mongo mongo;
	private final String dbname;
	private final String indexName;
	private final boolean sharded ;
	private final int blockSize;
	private final boolean compressed;
	
	public static final int DEFAULT_BLOCK_SIZE = 1024 * 128;
	public static final int DEFAULT_DIRTY_BLOCK_MAX = 12500;
	
	private NameToFile nameToFile ;
	
	private static Cache<MongoBlock, MongoBlock> dirtyBlockCache;
	
	public static void setDirtyCacheSize(int blocks) {
		ConcurrentMap<MongoBlock, MongoBlock> oldMap = null;
		if (dirtyBlockCache != null) {
			oldMap = dirtyBlockCache.asMap();
		}
		RemovalListener<MongoBlock, MongoBlock> listener = new RemovalListener<MongoBlock, MongoBlock>() {
			
			public void onRemoval(RemovalNotification<MongoBlock, MongoBlock> notification) {
				if (RemovalCause.SIZE.equals(notification.getCause())) {
					MongoBlock mb = notification.getKey();
					MongoFile mf = mb.getMongoFile();
					if (mb.isDirty()) {
						mf.storeBlock(mb);
					}
				}
			}
		};
		dirtyBlockCache = CacheBuilder.newBuilder().maximumSize(blocks).removalListener(listener).build();
		if (oldMap != null) {
			dirtyBlockCache.asMap().putAll(oldMap);
			oldMap.clear();
		}
	}
	
	static {
		setDirtyCacheSize(DEFAULT_DIRTY_BLOCK_MAX);
	}
	
	public static void cacheDirtyBlock(MongoBlock mb) {
		dirtyBlockCache.put(mb, mb);
	}
	
	public static void removeDirtyBlock(MongoBlock mb) {
		dirtyBlockCache.invalidate(mb);
	}

	public MongoDirectory(Mongo mongo, String dbname, String indexName) throws MongoException, IOException {
		this(mongo, dbname, indexName, false, false);
	}
	
	public MongoDirectory(Mongo mongo, String dbname, String indexName, boolean sharded, boolean compressed) throws MongoException, IOException {
		this(mongo, dbname, indexName, sharded, compressed, DEFAULT_BLOCK_SIZE);
	}
	
	private MongoDirectory(Mongo mongo, String dbname, String indexName, boolean sharded, boolean compressed, int blockSize) throws MongoException, IOException {
		this.compressed = compressed;
		this.mongo = mongo;
		this.dbname = dbname;
		this.indexName = indexName;
		this.sharded = sharded ;
		this.blockSize = blockSize;
		
		getFilesCollection().ensureIndex(FILE_NUMBER);
		getBlocksCollection().ensureIndex(FILE_NUMBER);
		
		DBObject indexes = new BasicDBObject();
		indexes.put(FILE_NUMBER, 1);
		indexes.put(BLOCK_NUMBER, 1);
		getBlocksCollection().ensureIndex(indexes);
		
		if (sharded) {
			String blockCollectionName = getBlocksCollection().getFullName();
			DB db = mongo.getDB(MongoConstants.StandardDBs.ADMIN);
			DBObject shardCommand = new BasicDBObject();
			shardCommand.put(MongoConstants.Commands.SHARD_COLLECTION, blockCollectionName);
			shardCommand.put(MongoConstants.Commands.SHARD_KEY, indexes);
			CommandResult cr = db.command(shardCommand);
			if (cr.getErrorMessage() != null) {
				System.err.println("Failed to shard <" + blockCollectionName + ">: " + cr.getErrorMessage());
			}
		}
		
		DBObject counter = new BasicDBObject();
		counter.put(_ID, FILE_COUNTER);
		
		DBCollection counterCollection = getCounterCollection();
		if (counterCollection.findOne(counter) == null) {
			counter.put(COUNTER, 0);
			counterCollection.insert(counter);
		}
		
		nameToFile = new NameToFile(this) ;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
	public DBCollection getCounterCollection() {
		DB db = mongo.getDB(dbname);
		DBCollection c = db.getCollection(indexName + COUNTER_SUFFIX);
		return c;
	}
	
	public DBCollection getFilesCollection() {
		DB db = mongo.getDB(dbname);
		DBCollection c = db.getCollection(indexName + FILES_SUFFIX);
		return c;
	}
	
	public DBCollection getBlocksCollection() {
		DB db = mongo.getDB(dbname);
		DBCollection c = db.getCollection(indexName + BLOCKS_SUFFIX);
		return c;
	}
	
	public String[] getFileNames() throws IOException {
		return nameToFile.getFileNames() ;
	}
	
	public MongoFile getFileHandle(String fileName) throws IOException {
		return getFileHandle(fileName, false);
	}
	
	public MongoFile getFileHandle(String filename, boolean createIfNotFound) throws IOException {
		
		MongoFile mfile = nameToFile.get(filename) ;
		if (mfile != null) return mfile ;
		
		
		DBCollection c = getFilesCollection();
		
		DBObject query = new BasicDBObject();
		query.put(FILE_NAME, filename);
		DBCursor cur = c.find(query);
		
		if (cur.hasNext()) {
			return loadFileFromDBObject(cur.next());
		}
		else if (createIfNotFound) {
			return createFile(filename);
		}
		
		throw new IOException("File not found: " + filename);
		
	}
	
	int getNewFileNumber() {
		DBCollection counterCollection = getCounterCollection();
		DBObject query = new BasicDBObject();
		query.put(_ID, FILE_COUNTER);
		DBObject update = new BasicDBObject();
		DBObject increment = new BasicDBObject();
		increment.put(COUNTER, 1);
		update.put($INC, increment);
		DBObject result = counterCollection.findAndModify(query, update);
		int count = (Integer) result.get(COUNTER);
		return count;
	}
	
	private MongoFile createFile(String fileName) throws IOException {
		return nameToFile.createFile(fileName) ;
	}
	
	boolean isCompressed() {
		return compressed;
	}

	MongoFile loadFileFromDBObject(DBObject dbObject) throws IOException {
		return nameToFile.loadFileFromDBObject(dbObject) ;
	}
	
	public static DBObject toDbObject(NosqlFile nosqlFile) throws IOException {
		try {
			DBObject dbObject = new BasicDBObject();
			dbObject.put(FILE_NUMBER, nosqlFile.getFileNumber());
			dbObject.put(FILE_NAME, nosqlFile.getFileName());
			dbObject.put(LENGTH, nosqlFile.getFileLength());
			dbObject.put(LAST_MODIFIED, nosqlFile.getLastModified());
			dbObject.put(BLOCK_SIZE, nosqlFile.getBlockSize());
			dbObject.put(COMPRESSED, nosqlFile.isCompressed());
			return dbObject;
		}
		catch (Exception e) {
			throw new IOException("Unable to serialize file descriptor for " + nosqlFile.getFileName() + e.getMessage());
		}
	}
	
	public int getBlockSize() {
		return blockSize;
	}
	
	public void updateFileMetadata(NosqlFile nosqlFile) throws IOException {
		
		DBCollection c = getFilesCollection();
		
		DBObject query = new BasicDBObject();
		query.put(FILE_NUMBER, nosqlFile.getFileNumber());
		
		DBObject object = toDbObject(nosqlFile);
		c.update(query, object, true, false, WriteConcern.SAFE);
		
	}
	
	public void deleteFile(NosqlFile nosqlFile) throws IOException {
		DBCollection c = getFilesCollection();
		
		DBObject query = new BasicDBObject();
		query.put(FILE_NUMBER, nosqlFile.getFileNumber());
		c.remove(query);
		
		DBCollection b = getBlocksCollection();
		b.remove(query, WriteConcern.SAFE);
		nameToFile.remove(nosqlFile.getFileName());
	}
	
	@Override
	public String toString() {
		return "MongoDirectory [dbname=" + dbname + ", indexName=" + indexName + ", blockSize=" + blockSize + "]";
	}

	public NosqlDirectory ifModified() throws IOException {
		
		if (nameToFile.isAllMatchFile()) return this ;

		return new MongoDirectory(mongo, dbname, indexName, this.sharded, this.compressed, this.blockSize) ;
	}
	
}


//class ReOpenFile {
//	
//	public void run(){
//		if (isModified()){
//			reopenMongoFileMap() ;
//		}
//	}
//}


class NameToFile {
	
	private ConcurrentHashMap<String, MongoFile> nameToFileMap = new ConcurrentHashMap<String, MongoFile>();

	private MongoDirectory mdir ;
	NameToFile(MongoDirectory mdir) throws MongoException, IOException {
		this.mdir = mdir ;
		fetchInitialContents() ;
	}

	synchronized void put(MongoFile mfile) throws MongoException, IOException {
		nameToFileMap.put(mfile.getFileName(), mfile);
	}

	synchronized void remove(String fileName) {
		nameToFileMap.remove(fileName) ;
	}

	synchronized void putIfAbsent(MongoFile mongoFile) {
		nameToFileMap.putIfAbsent(mongoFile.getFileName(), mongoFile) ;
	}

	synchronized MongoFile get(String filename) {
		return nameToFileMap.get(filename);
	}
	
	boolean isAllMatchFile() throws MongoException, IOException{
		List<MongoFile> newFileList = fetchFileList() ;
		return new HashSet<MongoFile>(nameToFileMap.values()).equals(new HashSet<MongoFile>(newFileList)) ;
	}

	synchronized String[] getFileNames() throws IOException {
//		nameToFileMap.clear();
//		fetchInitialContents() ;
		return nameToFileMap.keySet().toArray(new String[0]);
	}
	
	synchronized MongoFile loadFileFromDBObject(DBObject dbObject) throws IOException {
		MongoFile mongoFile = fromDbObject(dbObject);
		putIfAbsent(mongoFile);
		return get(mongoFile.getFileName());
	}
	
	synchronized MongoFile createFile(String fileName) throws IOException {
		MongoFile mongoFile = new MongoFile(mdir, fileName, mdir.getNewFileNumber(), mdir.getBlockSize(), mdir.isCompressed());
		mdir.updateFileMetadata(mongoFile);
		
		putIfAbsent(mongoFile);
		return get(mongoFile.getFileName());
	}

	List<MongoFile> fetchFileList() throws MongoException, IOException{
		List<MongoFile> result = ListUtil.newList() ;
		DBCollection c = mdir.getFilesCollection();
		DBObject query = new BasicDBObject();
		
		DBCursor cursor = c.find(query);
		while (cursor.hasNext()) {
			MongoFile mf = loadFileFromDBObject(cursor.next());
			result.add(mf) ;
		}
		cursor.close() ;
		return result ;
	}
	
	private void fetchInitialContents() throws MongoException, IOException {
		for (MongoFile mf : fetchFileList()) {
			nameToFileMap.put(mf.getFileName(), mf);
		}
	}

	private MongoFile fromDbObject(DBObject dbObject) throws IOException {
		try {
			MongoFile mongoFile = new MongoFile(mdir, (String) dbObject.get(mdir.FILE_NAME), (Integer) dbObject.get(mdir.FILE_NUMBER), (Integer) dbObject.get(mdir.BLOCK_SIZE), (Boolean) dbObject.get(mdir.COMPRESSED));
			mongoFile.setFileLength((Long) dbObject.get(mdir.LENGTH));
			mongoFile.setLastModified((Long) dbObject.get(mdir.LAST_MODIFIED));
			return mongoFile;
		}
		catch (Exception e) {
			throw new IOException("Unable to de-serialize file descriptor from: <" + dbObject + ">: " + e.getMessage());
			
		}
	}

}



