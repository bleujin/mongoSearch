package net.ion.isearcher.indexer.storage.mongo;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

public class DistributedIndexInput extends IndexInput {
	private final NosqlFile nosqlFile;
	
	private long position;
	
	public DistributedIndexInput(NosqlFile nosqlFile) {
		super(DistributedIndexInput.class.getSimpleName() + "(" + nosqlFile.getFileName() + ")");
		this.nosqlFile = nosqlFile;
	}
	
	@Override
	public void close() throws IOException {
		
	}
	
	@Override
	public long getFilePointer() {
		return position;
	}
	
	@Override
	public void seek(long pos) throws IOException {
		this.position = pos;
		
	}
	
	@Override
	public long length() {
		return nosqlFile.getFileLength();
	}
	
	@Override
	public byte readByte() throws IOException {
		return nosqlFile.readByte(position++);
		
	}
	
	@Override
	public void readBytes(byte[] b, int offset, int length) throws IOException {
		nosqlFile.readBytes(position, b, offset, length);
		position += length;
	}
	
}