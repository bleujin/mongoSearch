package net.ion.nsearcher.indexer.storage.mongo;

public class MongoBlock {
	
	private MongoFile mongoFile;
	private final int fileNumber;
	private final int blockNumber;
	private final byte[] bytes;
	
	private boolean dirty;
	private String indexName;
	
	public MongoBlock(MongoFile mongoFile, int fileNumber, int blockNumber, byte[] bytes) {
		this.mongoFile = mongoFile;
		this.indexName = mongoFile.getMongoDirectory().getIndexName();
		this.fileNumber = fileNumber;
		this.blockNumber = blockNumber;
		this.bytes = bytes;
		dirty = false;
	}
	
	public MongoFile getMongoFile() {
		return mongoFile;
	}
	
	public boolean isDirty() {
		return dirty;
	}
	
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
	public int getFileNumber() {
		return fileNumber;
	}
	
	public int getBlockNumber() {
		return blockNumber;
	}
	
	public byte[] getBytes() {
		return bytes;
	}
	
	public byte getByte(int blockOffset) {
		return bytes[blockOffset];
	}
	
	public void setByte(int blockOffset, byte b) {
		bytes[blockOffset] = b;
		dirty = true;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + blockNumber;
		result = prime * result + fileNumber;
		result = prime * result + indexName.hashCode();
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MongoBlock other = (MongoBlock) obj;
		if (blockNumber != other.blockNumber)
			return false;
		if (fileNumber != other.fileNumber)
			return false;
		if (!indexName.equals(other.indexName))
			return false;
		return true;
	}
	
}
