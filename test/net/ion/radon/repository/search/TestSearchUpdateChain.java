package net.ion.radon.repository.search;

import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.MapUtil;
import net.ion.radon.core.PageBean;

public class TestSearchUpdateChain extends TestBaseSearch {

	private void addNode(int index) {
		session.createQuery().remove();
		for (int i : ListUtil.rangeNum(index)) {
			session.newNode().put("name", "bleujin").put("age", 20).put("index", i);
		}
		session.commit();
	}

	public void testDropSpace() throws Exception {
		addNode(1);
		session.dropWorkspace() ;
		session.waitForFlushed() ;
		
		assertEquals(0, session.createSearchQuery().find().size());
	}
	
	public void testChainUpdate() throws Exception {
		addNode(2);

		session.createQuery().eq("index", 0).updateChain()
			.put("name", "hero").inc("ival", 1)
			.inlist("address", MapUtil.chainKeyMap().put("city", "seoul").put("ga", 113)).update();
		session.waitForFlushed() ;

		assertEquals(1, session.createQuery().eq("name", "hero").find().count());
		assertEquals(1, session.createSearchQuery().term("name", "hero").find("").size());
		assertEquals(1, session.createSearchQuery().term("ival", "1").find("").size());

	}
	
	public void testChainUpdateMulti() throws Exception {
		addNode(2);

		session.createQuery().updateChain()
			.put("name", "hero").inc("ival", 1)
			.inlist("address", MapUtil.chainKeyMap().put("city", "seoul").put("ga", 113)).update();
		session.waitForFlushed() ;

		assertEquals(2, session.createQuery().eq("name", "hero").find().count());
		assertEquals(2, session.createSearchQuery().term("name", "hero").find("").size());
		assertEquals(2, session.createSearchQuery().term("ival", "1").find("").size());
	}
	
	public void testChainMerge() throws Exception {
		session.dropWorkspace() ;
		
		session.createQuery().eq("index", 0).updateChain()
			.put("name", "hero").inc("ival", 1)
			.inlist("address", MapUtil.chainKeyMap().put("city", "seoul").put("ga", 113)).merge();
		session.waitForFlushed() ;

		assertEquals(1, session.createQuery().eq("name", "hero").find().count());
		assertEquals(1, session.createSearchQuery().term("name", "hero").find("").size());
	}

	
	
	
}
