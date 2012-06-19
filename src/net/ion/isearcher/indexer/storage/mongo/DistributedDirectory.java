package net.ion.isearcher.indexer.storage.mongo;

import java.io.IOException;
import java.sql.Ref;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;

public class DistributedDirectory extends Directory {
	
	protected NosqlDirectory nosqlDirectory ;
	
	public DistributedDirectory(NosqlDirectory nosqlDirectory) throws IOException {
		this(nosqlDirectory, RefreshStrategy.READ_ONLY) ;
	}

	public DistributedDirectory(NosqlDirectory nosqlDirectory, RefreshStrategy refreshStrategy) throws IOException {
		this.nosqlDirectory = nosqlDirectory;
		this.setLockFactory(new SingleInstanceLockFactory());
		refreshStrategy.setDir(this, nosqlDirectory) ;
		refreshStrategy.startDetect() ;
	}
	
	@Override
	public IndexOutput createOutput(String filename) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(filename, true);
		return new DistributedIndexOutput(nosqlFile);
	}
	
	void changeDir(NosqlDirectory nosqlDir){
		this.nosqlDirectory = nosqlDir ;
	}
	
	@Override
	public IndexInput openInput(String filename) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(filename);
		return new DistributedIndexInput(nosqlFile);
	}
	
	@Override
	public String[] listAll() throws IOException {
		ensureOpen();
		return nosqlDirectory.getFileNames();
	}
	
	@Override
	public boolean fileExists(String fileName) throws IOException {
		ensureOpen();
		try {
			return fileLength(fileName) >= 0;
		}
		catch (IOException e) {
			return false;
		}
	}
	
	@Override
	public long fileLength(String fileName) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(fileName);
		return nosqlFile.getFileLength();
	}
	
	@Override
	public long fileModified(String filename) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(filename);
		return nosqlFile.getLastModified();
	}
	
	@Override
	public void touchFile(String fileName) throws IOException {
		ensureOpen();
		try {
			NosqlFile nosqlFile = nosqlDirectory.getFileHandle(fileName);
			nosqlFile.setLastModified(System.currentTimeMillis());
			nosqlDirectory.updateFileMetadata(nosqlFile);
		}
		catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	
	@Override
	public void deleteFile(String fileName) throws IOException {
		ensureOpen();
		NosqlFile nosqlFile = nosqlDirectory.getFileHandle(fileName);
		nosqlDirectory.deleteFile(nosqlFile);
	}
	
	@Override
	public void close() throws IOException {
		isOpen = false;
	}
	
	@Override
	public String toString() {
		return nosqlDirectory.toString();
	}
}