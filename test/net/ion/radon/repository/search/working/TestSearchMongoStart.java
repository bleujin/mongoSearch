package net.ion.radon.repository.search.working;

import net.ion.framework.util.ListUtil;
import net.ion.framework.util.MapUtil;
import net.ion.nsearcher.search.SearchResponse;
import net.ion.radon.repository.search.TestBaseSearch;

public class TestSearchMongoStart extends TestBaseSearch{

	private void addNode(int index) {
		session.createQuery().remove() ;
		for (int i : ListUtil.rangeNum(index)) {
			session.newNode().put("name", "bleujin").put("age", 20).put("index", i) ;
		}
		session.commit() ;
	}
	

	public void testCreate() throws Exception {
		addNode(1);
		
		assertEquals(1, session.createQuery().find().count()) ;
		SearchResponse response = session.createSearchQuery().term("name", "bleujin").find("") ; 
		assertEquals(1, response.getTotalCount()) ;
		
		
		assertEquals(1, session.createSearchQuery().find("bleujin").getTotalCount()) ;
		assertEquals(1, session.createSearchQuery().term("index", "0").find("").getTotalCount()) ;
	}

	public void testDelete() throws Exception {
		addNode(2);

		assertEquals(2, session.createQuery().find().count()) ;
		assertEquals(2, session.createSearchQuery().term("name", "bleujin").find("").getTotalCount()) ;

		session.createQuery().lte("index", 0).remove() ;
		
		assertEquals(1, session.createQuery().find().count()) ;
		assertEquals(1, session.createSearchQuery().term("name", "bleujin").find("").getTotalCount()) ;
	}
	
	public void testUpdateChain() throws Exception {
		addNode(2);
		
		session.createQuery().eq("index", 0).updateChain().put("name", "hero")
			.inc("ival", 1)
			.inlist("address", MapUtil.chainKeyMap().put("city", "seoul").put("ga",113)).update() ;
		
		assertEquals(1, session.createQuery().eq("name", "hero").find().count()) ;
		assertEquals(1, session.createSearchQuery().term("name", "hero").find("").getTotalCount()) ;
		assertEquals(1, session.createSearchQuery().term("ival", "1").find("").getTotalCount()) ;
	}
	
	
	
}
