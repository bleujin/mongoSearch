package net.ion.radon.repository.search;

import net.ion.framework.db.Page;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.MapUtil;

public class TestSearchFind extends TestBaseSearch{

	private void addNode(int index) {
		session.createQuery().remove() ;
		for (int i : ListUtil.rangeNum(index)) {
			session.newNode().put("name", "bleujin").put("age", 20 + (i *5)).put("index", i).put("text", "hello hero")
				.inner("address").put("city", (i%2 == 0) ? "seoul" : "busan").put("bun", 1535).getParent()
				.inlist("friend").push(MapUtil.chainKeyMap().put("name", "novision").put("age", 30))
					.push(MapUtil.chainKeyMap().put("name", "iihi").put("age", 32)) ;
		}
		session.commit() ;
		session.waitForFlushed() ;
	}
	
	public void testTerm() throws Exception {
		addNode(2) ;
		
		assertEquals(2, session.createSearchQuery().term("name", "bleujin").find().getTotalCount()) ; 
		assertEquals(1, session.createSearchQuery().term("index", "0").find().getTotalCount()) ; 
		assertEquals(2, session.createSearchQuery().term("text", "hero").find().getTotalCount()) ; 
	}
	
	public void testLessThen() throws Exception {
		addNode(2) ;
		
		assertEquals(1, session.createSearchQuery().lt("index", 1).find().getTotalCount()) ;
		assertEquals(1, session.createSearchQuery().lt("index", 1L).find().getTotalCount()) ;
		assertEquals(2, session.createSearchQuery().lte("name", "bleuz").find().getTotalCount()) ;
	}

	public void testGreaterThen() throws Exception {
		addNode(2) ;
		
		assertEquals(1, session.createSearchQuery().gt("index", 0).find().getTotalCount()) ;
		assertEquals(1, session.createSearchQuery().gt("index", 0L).find().getTotalCount()) ;
		assertEquals(2, session.createSearchQuery().gte("name", "abcd").find().getTotalCount()) ;
	}
	

	public void testBetween() throws Exception {
		addNode(2) ;
		assertEquals(1, session.createSearchQuery().between("age", 20, 22).find().getTotalCount()) ;
		assertEquals(1, session.createSearchQuery().between("age", 25, 30).find().getTotalCount()) ;
		assertEquals(2, session.createSearchQuery().between("friend.age", 30, 30).find().getTotalCount()) ;
	}
	
	public void testBetweenTerm() throws Exception {
		addNode(2) ;
		assertEquals(1, session.createSearchQuery().between("address.city", "seoul", "seoul").find().getTotalCount()) ;
		assertEquals(2, session.createSearchQuery().between("address.city", "busan", "seoul").find().getTotalCount()) ;
	}
	
	
	
	public void testSort() throws Exception {
		addNode(2) ;
		
		assertEquals(0, session.createSearchQuery().ascending("index").find().getDocument().get(0).getAsLong("index")) ; 
		assertEquals(1, session.createSearchQuery().descending("index").find().getDocument().get(0).getAsLong("index")) ;
	}

	
	
	
}
