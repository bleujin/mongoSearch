package net.ion.radon.repository;

import java.io.IOException;
import java.util.Map.Entry;

import net.ion.isearcher.impl.Central;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.radon.repository.myapi.ICredential;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class SearchRepositoryCentral implements RCentral {

	private Central central ;
	private Mongo mongo;
	private String currentDBName = "test";
	private ICredential credential = SimpleCredential.BLANK;
	
	public SearchRepositoryCentral(Mongo mongo, String dbName, String userId, String userPwd, Directory dir) {
		this.mongo = mongo;
		this.currentDBName = dbName;
		this.credential = BasicCredential.create(userId, userPwd) ;
		this.central =  Central.createOrGet(dir);
	}

	public static SearchRepositoryCentral testCreate() throws MongoException, IOException {
		// StorageFac.createToMongo("61.250.201.78", "itest", "myin")
		return new SearchRepositoryCentral(new Mongo("61.250.201.78"), "test", null, null, new RAMDirectory()) ;
	}

	public SearchSession login(String defaultWorkspace) throws IllegalArgumentException {
		return login(currentDBName, defaultWorkspace);
	}
	public SearchSession login(String defaultWorkspace, Analyzer analyzer) {
		return login(currentDBName, defaultWorkspace, analyzer);
	}

	public SearchSession login(String dbName, String defaultWorkspace) throws IllegalArgumentException {
		return login(dbName, defaultWorkspace, new MyKoreanAnalyzer()) ;
	}
	
	public SearchSession login(String dbName, String defaultWorkspace, Analyzer analyzer) throws IllegalArgumentException {
		try {
			DB db = mongo.getDB(dbName);
			if (!credential.isAuthenticated(db)) throw new IllegalArgumentException("Authenticate is false");
			Session inner = LocalSession.create(LocalRepository.create(db), defaultWorkspace);
			return SearchSession.create(inner, central, analyzer);
		} catch (Throwable e) {
			throw new IllegalArgumentException(e) ;
		}
	}

	public void unload() {
		if (mongo != null) mongo.close();
	}

	public SearchSession testLogin(String defaultWorkspace) {
		return login("test", defaultWorkspace) ;
	}

}
