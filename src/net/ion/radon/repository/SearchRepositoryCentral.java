package net.ion.radon.repository;

import java.io.IOException;

import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.config.CentralConfig;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;
import net.ion.radon.repository.myapi.ICredential;

import org.apache.lucene.analysis.Analyzer;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class SearchRepositoryCentral implements RCentral {

	private Central central ;
	private Mongo mongo;
	private String currentDBName = "test";
	private ICredential credential = SimpleCredential.BLANK;
	
	public SearchRepositoryCentral(Mongo mongo, String dbName, String userId, String userPwd, Central central) {
		this.mongo = mongo;
		this.currentDBName = dbName;
		this.credential = BasicCredential.create(userId, userPwd) ;
		this.central = central ;
	}

	public static SearchRepositoryCentral testCreate() throws MongoException, IOException {
		// StorageFac.createToMongo("61.250.201.78", "itest", "myin")
		return new SearchRepositoryCentral(new Mongo("61.250.201.78"), "test", null, null, CentralConfig.newRam().build()) ;
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
		central.destroySelf() ;
		if (mongo != null) mongo.close();
	}

	public SearchSession testLogin(String defaultWorkspace) {
		return login("test", defaultWorkspace) ;
	}

}
