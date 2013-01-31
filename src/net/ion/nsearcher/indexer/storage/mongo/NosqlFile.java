package net.ion.nsearcher.indexer.storage.mongo;

import java.io.IOException;

public interface NosqlFile {
	
	public int getFileNumber();
	
	public String getFileName();
	
	public long getFileLength();
	
	public void setFileLength(long fileLength);
	
	public long getLastModified();
	
	public void setLastModified(long currentTime);
	
	public void write(long position, byte b) throws IOException;
	
	public void write(long position, byte[] b, int offset, int length) throws IOException;
	
	public void flush() throws IOException;
	
	public byte readByte(long position) throws IOException;
	
	public void readBytes(long position, byte[] b, int offset, int length) throws IOException;
	
	public int getBlockSize();
	
	public boolean isCompressed();
	
}