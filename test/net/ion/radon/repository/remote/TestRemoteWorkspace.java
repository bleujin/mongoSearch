package net.ion.radon.repository.remote;

import java.util.List;

import net.ion.framework.util.Debug;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeCursor;
import net.ion.radon.repository.NodeObject;
import net.ion.radon.repository.PropertyFamily;
import net.ion.radon.repository.RemoteSession;

public class TestRemoteWorkspace extends TestBaseRemote{


	public void testChangeWorkspace() throws Exception {
		RemoteSession session = remoteLogin() ;
		String currentWName = session.getCurrentWorkspaceName() ;
		session.dropWorkspace() ;
		
		session.newNode().put("name", "bleujin").getSession().commit() ;
		assertEquals(1, session.createQuery().eq("name", "bleujin").find().count()) ;
		
		session.changeWorkspace("newtest") ;
		session.dropWorkspace() ;
		
		assertEquals("newtest", session.getCurrentWorkspaceName()) ;
		assertEquals(1, session.createQuery(currentWName).eq("name", "bleujin").find().count()) ;
		
		assertEquals(0, session.createQuery().find().count()) ;
		session.newNode().put("name", "hero").getSession().commit() ;
		
		assertEquals(1, session.createQuery().find().count()) ;
	}
	
	public void testOtherWorkspaceSearch() throws Exception {
		RemoteSession session = remoteLogin() ;
		String currentWName = session.getCurrentWorkspaceName() ;
		session.dropWorkspace() ;
		
		session.newNode().put("index", 0).put("name", "bleujin").getSession().commit() ;
		
		session.changeWorkspace("newtest") ;
		session.dropWorkspace() ;
		session.newNode().put("index", 1).put("name", "bleujin").getSession().commit() ;
		
		
		assertEquals(1, session.createQuery().find().count()) ;
	}
	
	public void testAradonId() throws Exception {
		RemoteSession session = remoteLogin() ;
		session.dropWorkspace() ;
		
		Node newNode = session.newNode().setAradonId("emp", 4040) .put("index", 0).put("name", "bleujin") ;
		newNode.getSession().commit() ;
		assertEquals(newNode.getIdentifier(), session.createQuery().findOne().getIdentifier()) ;
	}
	
	public void testMakeIndex() throws Exception {
		RemoteSession session = remoteLogin() ;
		session.dropWorkspace() ;
		session.newNode().setAradonId("emp", 4040) .put("index", 0).put("name", "bleujin").getSession().commit() ;

		session.getCurrentWorkspace().makeIndex(PropertyFamily.create("name", 1), "test_index_name", false) ;
		
		List<NodeObject> infos = session.getCurrentWorkspace().getIndexInfo() ;
		
		boolean found = false ;
		for (NodeObject info : infos) {
			if (info.get("name").equals("test_index_name")){
				found = true ;
			} 
		} 
		assertEquals(true, found) ;
		
		NodeCursor nc =  session.createQuery().eq("name", "bleujin").find() ;
		assertEquals(true, nc.explain().useIndex()) ;
	}

}
