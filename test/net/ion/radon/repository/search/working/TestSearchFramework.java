package net.ion.radon.repository.search.working;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyDocument.Action;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import junit.framework.TestCase;

public class TestSearchFramework extends TestCase{

	
	public void testUpdateIndex() throws Exception {
		Central cen = Central.createOrGet(new RAMDirectory()) ;
		
		IWriter wr = cen.newIndexer(new MyKoreanAnalyzer()) ;
		wr.begin("test") ;
		Action action = wr.updateDocument(MyDocument.testDocument().keyword("name", "bleujin")) ;
		wr.end() ;
		
		assertEquals(1, cen.newSearcher().searchTest("bleujin").getTotalCount());
		assertEquals(Action.Update, action) ;
	}
	
	public void testIn() throws Exception {
		RepositoryCentral rc = RepositoryCentral.testCreate() ;
		Session session = rc.login("test") ;
		session.dropWorkspace() ;
		
		session.newNode().put("name", "bleujin") ;
		session.commit() ;
		
		session.createQuery().in("name", new String[]{"bleujin"}).find().debugPrint(PageBean.ALL) ;
	}
}
