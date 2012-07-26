package net.ion.isearcher.indexer.storage.mongo;


import java.io.IOException;

import com.mongodb.MongoException;

public interface NosqlDirectory {
	
	public String[] getFileNames() throws IOException;
	
	public NosqlFile getFileHandle(String fileName) throws IOException;
	
	public NosqlFile getFileHandle(String fileName, boolean createIfNotFound) throws IOException;
	
	public int getBlockSize();
	
	public void updateFileMetadata(NosqlFile nosqlFile) throws IOException;
	
	public void deleteFile(NosqlFile nosqlFile) throws IOException;
	
	public NosqlDirectory ifModified() throws IOException ;
	
	public NosqlDirectory newDir() throws IOException, MongoException ;
	
}