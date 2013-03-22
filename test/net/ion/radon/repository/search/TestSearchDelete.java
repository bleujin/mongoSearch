package net.ion.radon.repository.search;

import net.ion.radon.repository.MergeQuery;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.TempNode;

public class TestSearchDelete  extends TestBaseSearch {

	
	public void testRemoveNode() throws Exception {
		session.createQuery().remove() ;
		
		session.newNode().put("name", "bleujin").getSession().commit() ;
		Node found = session.createQuery().eq("name", "bleujin").findOne() ; 
		
		assertEquals(true, found != null) ;
		
		session.remove(found) ;
		session.commit() ;
		session.waitForFlushed() ;

		assertEquals(0, session.createQuery().find().count()) ;
		assertEquals(0, session.createSearchQuery().find().size()) ;
	}
}
