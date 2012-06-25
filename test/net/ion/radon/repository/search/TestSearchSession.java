package net.ion.radon.repository.search;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;

import net.ion.framework.util.CaseInsensitiveHashMap;
import net.ion.framework.util.DateFormatUtil;
import net.ion.framework.util.Debug;
import net.ion.isearcher.impl.Central;
import net.ion.radon.repository.IndexInfoHandler;
import net.ion.radon.repository.PropertyQuery;
import net.ion.radon.repository.SearchSession;

public class TestSearchSession extends TestBaseSearch{

	public void testSync() throws Exception {
		session.dropWorkspace() ;
		session.newNode().put("name", "bleujin").getSession().commit() ;
		
		session.waitForFlushed() ;
		assertEquals(1, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ; 
		
		old.createQuery().eq("name", "bleujin").updateChain().put("name", "hero").update() ;
		
		
		assertEquals(1, old.createQuery().eq("name", "hero").find().count());
		assertEquals(1, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ; // not applied
		
		session.resyncIndex(PropertyQuery.create().eq("name", "hero")) ;
		session.waitForFlushed() ;
		
		assertEquals(0, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ; 
		assertEquals(1, session.createSearchQuery().term("name", "hero").find().getTotalCount()) ; 
	}
	
	public void testSyncLimit() throws Exception {
		session.dropWorkspace() ;
		session.newNode().put("name", "bleujin").getSession().
				newNode().put("name", "jin").getSession().commit() ;
		
		session.waitForFlushed() ;
		assertEquals(1, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ; 
		
		old.createQuery().eq("name", "bleujin").updateChain().put("name", "hero").update() ;
		
		
		assertEquals(1, old.createQuery().eq("name", "hero").find().count());
		assertEquals(1, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ; // not applied
		assertEquals(1, session.createSearchQuery().term("name", "jin").find().getTotalCount()) ;
		
		Future<Integer> future = session.resyncIndex(PropertyQuery.create().eq("name", "hero")) ;
		assertTrue(future.get().equals(1)) ;
		
		assertEquals(0, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ; 
		assertEquals(1, session.createSearchQuery().term("name", "hero").find().getTotalCount()) ; 
		assertEquals(1, session.createSearchQuery().term("name", "jin").find().getTotalCount()) ; 
	}
	
	
	public void testIndexInfo() throws Exception {

		session.dropWorkspace() ;


		int coiunt = session.getIndexInfo(new IndexInfoHandler<Integer>() {
			public Integer handle(SearchSession session, Central central) {
				try {
					return session.createSearchQuery().find().getTotalCount();
				} catch (IOException e) {
					return 0 ;
				} catch (ParseException e) {
					return 0 ;
				}
			}
		}) ;

		Map<String, Object> map = session.getIndexInfo(IndexInfo) ;
		Debug.debug(map) ;
	}
	
	static IndexInfoHandler<Map<String, Object>> IndexInfo = new IndexInfoHandler<Map<String, Object>>() {
		public Map<String, Object> handle(SearchSession session, Central central) {
			final Map<String, Object> map = new CaseInsensitiveHashMap<Object>();
			try {
				IndexReader reader = central.newReader().getIndexReader();
				map.put("numDoc", session.createSearchQuery().find().getTotalCount());
				map.put("allNumDoc", reader.numDocs());
				map.put("indexPath", central.getDir().toString());
				map.put("lastModified", DateFormatUtil.date2String(new Date(IndexReader.lastModified(central.getDir())), "yyyy-MM-dd HH:mm:ss"));
			} catch (Exception ex) {
				map.put("numDoc", 0);
				map.put("allNumDoc", 0);
				map.put("indexPath", "");
				map.put("lastModified", "");
			} 
			return map ;
		}
	};

}