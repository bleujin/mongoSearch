package net.ion.nsearcher.indexer.storage.mongo;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;

import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.config.CentralConfig;

public class SimpleCentralConfig extends CentralConfig {

	
	private Directory dir;
	private SimpleCentralConfig(Directory dir) {
		this.dir = dir ;
	}

	public final static SimpleCentralConfig create(Directory dir){
		return new SimpleCentralConfig(dir) ;
	}
	
	@Override
	public Directory buildDir() throws IOException {
		return dir;
	}

	public static Central createCentral(Directory dir) throws CorruptIndexException, IOException {
		return create(dir).build();
	}

}
