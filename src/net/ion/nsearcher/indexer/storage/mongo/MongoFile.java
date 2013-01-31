package net.ion.nsearcher.indexer.storage.mongo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.ion.framework.util.Debug;
import net.ion.nsearcher.indexer.storage.mongo.Compression.CompressionLevel;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MongoFile implements NosqlFile {
	
	private final MongoDirectory mongoDirectory;
	private final String fileName;
	private final int fileNumber;
	private final int blockSize;
	
	private long fileLength;
	private long lastModified;
	
	private MongoBlock currentReadBlock;
	private MongoBlock currentWriteBlock;
	
	private ConcurrentMap<Integer, Lock> blockLocks;
	
	private Cache<Integer, MongoBlock> dirtyBlocks;
	private LoadingCache<Integer, MongoBlock> cache;
	private boolean compressed;
	
	protected MongoFile(MongoDirectory mongoDirectory, String fileName, int fileNumber, int blockSize, boolean compressed) {
		
		this.mongoDirectory = mongoDirectory;
		this.fileName = fileName;
		this.fileNumber = fileNumber;
		this.fileLength = 0;
		this.lastModified = System.currentTimeMillis();
		
		this.blockSize = blockSize;
		this.compressed = compressed;
		
		this.blockLocks = new ConcurrentHashMap<Integer, Lock>();
		
		RemovalListener<Integer, MongoBlock> removalListener = new RemovalListener<Integer, MongoBlock>() {
			
			public void onRemoval(RemovalNotification<Integer, MongoBlock> notification) {
				if (RemovalCause.EXPLICIT.equals(notification.getCause())) {
					Integer key = notification.getKey();
					Lock l = blockLocks.get(key);
					l.lock();
					try {
						
						MongoBlock mb = notification.getValue();
						
						if (mb.isDirty()) {
							storeBlock(mb);
							MongoDirectory.removeDirtyBlock(mb);
						}
						
					}
					finally {
						l.unlock();
					}
				}
				
			}
		};
		
		CacheLoader<Integer, MongoBlock> cacheLoader = new CacheLoader<Integer, MongoBlock>() {
			@Override
			public MongoBlock load(Integer key) throws Exception {
				if (!blockLocks.containsKey(key)) {
					blockLocks.putIfAbsent(key, new ReentrantLock());
				}
				
				Lock l = blockLocks.get(key);
				try {
					l.lock();
					MongoBlock mb = dirtyBlocks.getIfPresent(key);
					if (mb != null) {
						return mb;
					}
					return getBlock(key, true);
				}
				catch (Exception e) {
					System.err.println("Exception in mongo block <" + key + "> for file <" + MongoFile.this + ">:" + e);
					e.printStackTrace();
					throw e;
				}
				finally {
					l.unlock();
				}
			}
		};
		
		this.dirtyBlocks = CacheBuilder.newBuilder().removalListener(removalListener).softValues().build();
		this.cache = CacheBuilder.newBuilder().softValues().build(cacheLoader);
	}
	
	public MongoDirectory getMongoDirectory() {
		return mongoDirectory;
	}
	
	public String getFileName() {
		return fileName;
	}
	
	public long getFileLength() {
		
		return fileLength;
	}
	
	public void setFileLength(long fileLength) {
		this.fileLength = fileLength;
	}
	
	public long getLastModified() {
		return lastModified;
	}
	
	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
	}
	
	public byte readByte(long position) throws IOException {
		try {
			// System.out.println("Read " + filename + ": position: " + position + " length: 1");
			int block = (int) (position / mongoDirectory.getBlockSize());
			int blockOffset = (int) (position - (block * mongoDirectory.getBlockSize()));
			
			MongoBlock mb = currentReadBlock;
			
			if (mb == null || block != mb.getBlockNumber()) {
				currentReadBlock = mb = cache.get(block);
			}
			
			return mb.getByte(blockOffset);
		}
		catch (ExecutionException e) {
			throw new IOException("Failed to read byte at position: " + position);
		}
	}
	
	public void readBytes(long position, byte[] b, int offset, int length) throws IOException {
		try {
			// System.out.println("Read " + filename + ": position: " + position + " length: " + length);
			
			while (length > 0) {
				int block = (int) (position / blockSize);
				int blockOffset = (int) (position - (block * blockSize));
				
				int readSize = Math.min(blockSize - blockOffset, length);
				
				MongoBlock mb = currentReadBlock;
				
				if (mb == null || block != mb.getBlockNumber()) {
					currentReadBlock = mb = cache.get(block);
				}
				
				System.arraycopy(mb.getBytes(), blockOffset, b, offset, readSize);
				
				position += readSize;
				offset += readSize;
				length -= readSize;
			}
		}
		catch (ExecutionException e) {
			throw new IOException("Failed to read bytes at position: " + position);
		}
	}
	
	public void write(long position, byte b) throws IOException {
		try {
			int block = (int) (position / blockSize);
			int blockOffset = (int) (position - (block * blockSize));
			
			// System.out.println("Write " + filename + ": position: " + position + " length: 1 block:" + block);
			
			MongoBlock mb = currentWriteBlock;
			
			if (mb == null || block != mb.getBlockNumber()) {
				if (mb != null) {
					markDirty(mb);
				}
				currentWriteBlock = mb = cache.get(block);
			}
			
			mb.setByte(blockOffset, b);
			mb.setDirty(true);
			
			fileLength = Math.max(position + 1, fileLength);
		}
		catch (ExecutionException e) {
			throw new IOException("Failed to write byte at position: " + position);
		}
	}
	
	public void write(long position, byte[] b, int offset, int length) throws IOException {
		try {
			// System.out.println("Write " + filename + ": position: " + position + " length: " + length);
			while (length > 0) {
				int block = (int) (position / blockSize);
				int blockOffset = (int) (position - (block * blockSize));
				int writeSize = Math.min(blockSize - blockOffset, length);
				
				MongoBlock mb = currentWriteBlock;
				
				if (mb == null || block != mb.getBlockNumber()) {
					if (mb != null) {
						markDirty(mb);
					}
					currentWriteBlock = mb = cache.get(block);
				}
				
				byte[] dest = mb.getBytes();
				System.arraycopy(b, offset, dest, blockOffset, writeSize);
				mb.setDirty(true);
				position += writeSize;
				offset += writeSize;
				length -= writeSize;
			}
			fileLength = Math.max(position + length, fileLength);
		}
		catch (ExecutionException e) {
			throw new IOException("Failed to write bytes at position: " + position);
		}
	}
	
	private void markDirty(MongoBlock mb) {
		int blockNumber = mb.getBlockNumber();
		Lock l = blockLocks.get(blockNumber);
		l.lock();
		try {
			MongoDirectory.cacheDirtyBlock(mb);
			dirtyBlocks.put(mb.getBlockNumber(), mb);
		}
		finally {
			l.unlock();
		}
	}
	
	public void flush() throws IOException {
		// System.out.println("Flush");
		
		dirtyBlocks.put(currentWriteBlock.getBlockNumber(), currentWriteBlock);
		
		Set<Integer> dirtyBlockKeys = new HashSet<Integer>(dirtyBlocks.asMap().keySet());
		
		dirtyBlocks.invalidateAll(dirtyBlockKeys);
		
		mongoDirectory.updateFileMetadata(this);
	}
	
	private MongoBlock getBlock(Integer blockNumber, boolean createIfNotExist) throws IOException {
		
		DBCollection c = mongoDirectory.getBlocksCollection();
		
		DBObject query = new BasicDBObject();
		query.put(MongoDirectory.FILE_NUMBER, fileNumber);
		query.put(MongoDirectory.BLOCK_NUMBER, blockNumber);
		
		DBObject result = c.findOne(query);
		
		byte[] bytes = null;
		if (result != null) {
			// System.out.println("Fetch: filename:" + fileName + " block: " + blockNumber);
			bytes = (byte[]) result.get(MongoDirectory.BYTES);
			boolean blockCompressed = (Boolean) result.get(MongoDirectory.COMPRESSED);
			if (blockCompressed) {
				bytes = Compression.uncompressZlib(bytes);
				// bytes = Snappy.uncompress(bytes);
			}
			
			return new MongoBlock(this, fileNumber, blockNumber, bytes);
		}
		
		if (createIfNotExist) {
			// System.out.println("Create: filename:" + fileName + " block: " + blockNumber);
			bytes = new byte[blockSize];
			MongoBlock mongoBlock = new MongoBlock(this, fileNumber, blockNumber, bytes);
			storeBlock(mongoBlock);
			return mongoBlock;
		}
		
		return null;
		
	}
	
	public void storeBlock(MongoBlock mongoBlock) {
		// System.out.println("Store: " + mongoBlock.getBlockNumber());
		
		DBCollection c = mongoDirectory.getBlocksCollection();
		
		DBObject query = new BasicDBObject();
		query.put(MongoDirectory.FILE_NUMBER, fileNumber);
		query.put(MongoDirectory.BLOCK_NUMBER, mongoBlock.getBlockNumber());
		
		DBObject object = new BasicDBObject();
		object.put(MongoDirectory.FILE_NUMBER, fileNumber);
		object.put(MongoDirectory.BLOCK_NUMBER, mongoBlock.getBlockNumber());
		byte[] orgBytes = mongoBlock.getBytes();
		byte[] newBytes = orgBytes;
		boolean blockCompressed = compressed;
		if (blockCompressed) {
			try {
				newBytes = Compression.compressZlib(orgBytes,  CompressionLevel.BEST);
				if (newBytes.length >= orgBytes.length) {
					System.out.println("Disabling compression for block <" + mongoBlock + "> compresion size <" + newBytes.length
							+ "> greater than or equals to old size <" + orgBytes.length + ">");
					newBytes = orgBytes;
					blockCompressed = false;
				}
			}
			catch (Exception e) {
				System.err.println("Failed to compress block : <" + mongoBlock + ">.  Storing uncompressed: " + e);
				blockCompressed = false;
			}
		}
		object.put(MongoDirectory.BYTES, newBytes);
		object.put(MongoDirectory.COMPRESSED, blockCompressed);
		
		c.update(query, object, true, false, WriteConcern.SAFE);
		mongoBlock.setDirty(false);
		
	}
	
	@Override
	public String toString() {
		return "MongoFile [fileName=" + fileName + ", fileNumber=" + fileNumber + ", blockSize=" + blockSize + ", fileLength=" + fileLength + ", lastModified=" + lastModified + ", compressed=" + compressed + "]";
	}
	
	public boolean equals(Object obj){
		if (!(obj instanceof MongoFile)) return false ;
		MongoFile that = (MongoFile) obj ;
		
		return fileName.equals(that.fileName) && fileNumber == that.fileNumber && fileLength == that.fileLength && lastModified == that.lastModified && compressed == that.compressed ; 
	}
	
	public int hashCode(){
		return toString().hashCode() ;
	}
	
	public int getFileNumber() {
		return fileNumber;
	}
	
	public int getBlockSize() {
		return blockSize;
	}
	
	public boolean isCompressed() {
		return compressed;
	}
}
