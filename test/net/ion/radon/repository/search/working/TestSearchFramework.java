package net.ion.radon.repository.search.working;

import junit.framework.TestCase;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyDocument.Action;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.config.CentralConfig;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;

public class TestSearchFramework extends TestCase{

	
	public void testUpdateIndex() throws Exception {
		Central cen = CentralConfig.newRam().build() ;
		
		Indexer wr = cen.newIndexer() ;
		
		Action action = wr.index(new IndexJob<Action>(){
			public Action handle(IndexSession session) throws Exception {
				Action action = session.updateDocument(MyDocument.testDocument().keyword("name", "bleujin")) ;
				return action;
			}
		}) ;
		
		assertEquals(1, cen.newSearcher().search("bleujin").size());
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
