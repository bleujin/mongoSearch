package net.ion.nsearcher.indexer.storage.mongo;

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

public class DistributedIndexOutput extends IndexOutput {
	
	private final NosqlFile nosqlFile;
	
	private boolean isOpen;
	private long position;
	
	public DistributedIndexOutput(NosqlFile nosqlFile) throws IOException {
		this.nosqlFile = nosqlFile;
		this.isOpen = true;
	}
	
	@Override
	public void close() throws IOException {
		if (isOpen) {
			flush();
			isOpen = false;
		}
	}
	
	@Override
	public long length() throws IOException {
		return nosqlFile.getFileLength();
	}
	
	@Override
	public void flush() throws IOException {
		nosqlFile.flush();
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
	public void writeByte(byte b) throws IOException {
		nosqlFile.write(position, b);
		position += 1;
	}
	
	@Override
	public void writeBytes(byte[] b, int offset, int length) throws IOException {
		nosqlFile.write(position, b, offset, length);
		position += length;
	}
	
	@Override
	public void setLength(long length) throws IOException {
		nosqlFile.setFileLength(length);
	}
	
}