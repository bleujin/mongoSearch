package net.ion.radon.repository.remote;

import net.ion.framework.util.ListUtil;
import net.ion.framework.util.MapUtil;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.RemoteSession;
import net.ion.radon.repository.Session;

public class TestRemoteUpdateChain extends TestBaseRemote{

	private void addNode(int index) {
		Session session = confirmLogin(RemoteTestWorkspaceName) ;
		session.createQuery().remove();
		for (int i : ListUtil.rangeNum(index)) {
			session.newNode().put("name", "bleujin").put("age", 20).put("index", i);
		}
		session.commit();
	}

	public void testDropSpace() throws Exception {
		addNode(1);
		Session session = remoteLogin() ;
		session.dropWorkspace() ;
		
		assertEquals(0, session.createQuery().find().count());
	}
	
	public void testChainUpdate() throws Exception {
		addNode(2);

		RemoteSession session = remoteLogin() ;
		session.createQuery().eq("index", 0).updateChain()
			.put("name", "hero").inc("ival", 1)
			.inlist("address", MapUtil.chainKeyMap().put("city", "seoul").put("ga", 113)).update();

		Node found = session.createQuery().eq("name", "hero").findOne() ;
		assertEquals(1, found.getAsInt("ival"));
		assertEquals("hero", found.get("name"));
		
		assertEquals("seoul", found.get("address.0.city")) ;
	}


	public void testChainUpdateMulti() throws Exception {
		addNode(2);

		RemoteSession session = remoteLogin() ;
		session.createQuery().updateChain()
			.put("name", "hero").inc("ival", 1)
			.inlist("address", MapUtil.chainKeyMap().put("city", "seoul").put("ga", 113)).update();

		assertEquals(2, session.createQuery().eq("name", "hero").find().count());
		assertEquals(1, session.createQuery().eq("name", "hero").findOne().getAsInt("ival"));
	}
	
	public void testChainMerge() throws Exception {
		RemoteSession session = remoteLogin() ;
		session.dropWorkspace() ;
		
		session.createQuery().eq("index", 0).updateChain()
			.put("name", "hero").inc("ival", 1)
			.inlist("address", MapUtil.chainKeyMap().put("city", "seoul").put("ga", 113)).merge();

		assertEquals(1, session.createQuery().eq("name", "hero").find().count());
	}

	
	
	
}
