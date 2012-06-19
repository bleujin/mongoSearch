package net.ion.radon.repository;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.radon.core.PageBean;

import com.mongodb.Mongo;

import junit.framework.TestCase;

public class TestSearchRepositoryCentral extends TestCase {

	public void testLogin() throws Exception {
		
		Mongo mongo = new Mongo("61.250.201.197");
		String dbName = "ICS_MONGO";
		String userId = "ics";
		String userPwd = "ics";

		Directory dir = new RAMDirectory();
		SearchRepositoryCentral sc = new SearchRepositoryCentral(mongo, dbName, userId, userPwd, dir);
		
		SearchSession ss = sc.login(dbName, "test") ;
		ss.dropWorkspace() ;
		ss.newNode().put("name", "bleujin").getSession().commit() ;

		ss.createQuery().find().debugPrint(PageBean.ALL) ;
		
		ss.waitForFlushed() ;
		ss.createSearchQuery().find().debugPrint(Page.ALL) ;
	}
}
