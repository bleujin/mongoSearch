package net.ion.radon.repository.search;

import java.io.File;

import javax.swing.plaf.ListUI;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import com.mongodb.Mongo;

import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.framework.util.StringUtil;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.ISearcher;
import net.ion.isearcher.indexer.storage.mongo.DistributedDirectory;
import net.ion.isearcher.indexer.storage.mongo.MongoDirectory;
import net.ion.isearcher.searcher.ISearchRequest;
import net.ion.isearcher.searcher.SearchRequest;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.SearchRepositoryCentral;
import net.ion.radon.repository.SearchSession;
import junit.framework.TestCase;

public class TestBigIndex extends TestCase{

	
	private SearchSession session ;
	private Central mycen;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Mongo mongo = new Mongo("61.250.201.78");
		Directory dir = new DistributedDirectory(new MongoDirectory(mongo, "test", "bidxifle")) ;
		
		SearchRepositoryCentral rc = new SearchRepositoryCentral(mongo, "test", null, null, dir) ;
		session = rc.login("search", "bigindex") ;
		
		mycen = Central.createOrGet(dir);
	}
	
	public void xtestIndex() throws Exception {
//		session.dropWorkspace() ;
		String buildString = IOUtil.toString(new File("resource/aradon/build_plugin.xml").toURI()) ;
		int loopMax = 300 ;
		for (int loop : ListUtil.rangeNum(loopMax)) {
			for (int i : ListUtil.rangeNum(1000)) {
				session.newNode().put("job", "bindex").put("index", i + 1000*loop).put("rand", RandomUtil.nextRandomString(20)).put("buildString", buildString) ;
			}
			session.commit() ;
			session.waitForFlushed() ;
		}
		
	}
	
	public void xtestSearch() throws Exception {
		//session.createQuery().find().debugPrint(PageBean.ALL) ;
		
		// Debug.line(session.createSearchQuery().term("index", "333").find().getTotalCount()) ;
		ISearcher searcher = mycen.newSearcher() ;
		
		
		searcher.searchTest("").debugPrint(Page.ALL) ;
		
	}
	
	
	
	
}
