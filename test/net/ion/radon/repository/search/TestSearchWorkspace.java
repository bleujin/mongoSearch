package net.ion.radon.repository.search;

import net.ion.framework.util.Debug;
import net.ion.radon.repository.NodeConstants;
import net.ion.radon.repository.NodeCursor;
import net.ion.radon.repository.PropertyFamily;
import net.ion.radon.repository.SearchQuery;


public class TestSearchWorkspace extends TestBaseSearch {

	public void testChangeWorkspace() throws Exception {
		String currentWName = session.getCurrentWorkspaceName() ;
		session.dropWorkspace() ;
		
		session.newNode().put("name", "bleujin").getSession().commit() ;
		session.waitForFlushed() ;
		
		
		assertEquals(1, session.createQuery().eq("name", "bleujin").find().count()) ;
		assertEquals(1, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ;
		
		session.changeWorkspace("newtest") ;
		session.dropWorkspace() ;
		session.waitForFlushed() ;
		
		assertEquals("newtest", session.getCurrentWorkspaceName()) ;
		assertEquals(1, session.createQuery(currentWName).eq("name", "bleujin").find().count()) ;
		
		assertEquals(0, session.createQuery().find().count()) ;
		session.newNode().put("name", "hero").getSession().commit() ;
		session.waitForFlushed() ;
		
		assertEquals(1, session.createQuery().find().count()) ;
	}
	
	public void testOtherWorkspaceSearch() throws Exception {
		String currentWName = session.getCurrentWorkspaceName() ;
		session.dropWorkspace() ;
		
		session.newNode().put("index", 0).put("name", "bleujin").getSession().commit() ;
		
		session.changeWorkspace("newtest") ;
		session.dropWorkspace() ;
		session.newNode().put("index", 1).put("name", "bleujin").getSession().commit() ;
		session.waitForFlushed() ;
		
		
		assertEquals(1, session.createQuery().find().count()) ;
		assertEquals(1, session.createSearchQuery().find().getTotalCount()) ;
		
		assertEquals(1, session.createSearchQuery().wsname(currentWName).find().getTotalCount()) ;
		assertEquals(1, session.createSearchQuery().wsname("newtest").find().getTotalCount()) ;
		assertEquals(2, session.createSearchQuery().inAllWorkspace().find().getTotalCount()) ;
		
		
		assertEquals(1, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ;
	}
	
	public void testAradonId() throws Exception {
		session.dropWorkspace() ;
		
		session.newNode().setAradonId("emp", 4040) .put("index", 0).put("name", "bleujin").getSession().commit() ;
		session.waitForFlushed() ;
		
		assertEquals(1, session.createSearchQuery().aradonGroup("emp").find().getTotalCount()) ;
		assertEquals(1, session.createSearchQuery().aradonUid(4040).find().getTotalCount()) ;
		assertEquals(1, session.createSearchQuery().aradonId("emp", 4040).find().getTotalCount()) ;
	}
	
	public void testMakeIndex() throws Exception {
		session.dropWorkspace() ;
		session.newNode().setAradonId("emp", 4040) .put("index", 0).put("name", "bleujin").getSession().commit() ;

		session.getCurrentWorkspace().makeIndex(PropertyFamily.create("name", 1), "test_index_name", false) ;
		session.waitForFlushed() ;
		
		NodeCursor nc =  session.createQuery().eq("name", "bleujin").find() ;
		assertEquals(true, nc.explain().useIndex()) ;
	}
}
