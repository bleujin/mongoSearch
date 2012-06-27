package net.ion.radon.repository.remote;

import java.util.Map;

import net.ion.framework.util.Debug;
import net.ion.framework.util.MapUtil;
import net.ion.isearcher.common.MyDocument;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeResult;
import net.ion.radon.repository.RemoteSession;
import net.ion.radon.repository.Session;

public class TestRemoteUpdate extends TestBaseRemote{

	public void testDelete() throws Exception {
		Session cs = super.confirmLogin(RemoteTestWorkspaceName) ;
		cs.createQuery().remove() ;

		cs.newNode().put("name", "bleujin").getSession()
			.newNode().put("name", "hero").getSession().commit() ;

		assertEquals(1, cs.createQuery().eq("name", "bleujin").count()) ; 
		
		RemoteSession session = super.remoteLogin() ;
		session.createQuery().eq("name", "bleujin").remove() ;
		
		session.createQuery().find().debugPrint(PageBean.ALL) ;
		
		assertEquals(0, session.createQuery().eq("name", "bleujin").find().count()) ; 
		assertEquals(1, session.createQuery().eq("name", "hero").find().count()) ; 
	}


	public void testDropWorkspace() throws Exception {
		Session cs = super.confirmLogin(RemoteTestWorkspaceName) ;
		
		cs.newNode().put("name", "bleujin").getSession()
			.newNode().put("name", "hero").getSession().commit() ;

		assertEquals(true, cs.createQuery().count() > 1) ; 
		
		RemoteSession session = super.remoteLogin() ;
		session.getCurrentWorkspace().drop() ;
		
		assertEquals(0, cs.createQuery().find().count()) ; 
	}
	
	
	public void testUpdate() throws Exception {
		Session cs = super.confirmLogin(RemoteTestWorkspaceName) ;
		cs.createQuery().remove() ;
		
		cs.newNode().put("name", "bleujin").getSession()
			.newNode().put("name", "hero").getSession().commit() ;
		
		RemoteSession session = super.remoteLogin() ;
		session.createQuery().eq("name", "bleujin").update(MapUtil.chainKeyMap().put("age", 20).put("city", "seoul")) ;
		
		cs.createQuery().find().debugPrint(PageBean.ALL) ;
	}
	
	
	
	public void testOverwriteOne() throws Exception {
		Session cs = super.confirmLogin(RemoteTestWorkspaceName) ;
		cs.createQuery().remove() ;
		Node savedNode = cs.newNode() ;
		savedNode.put("name", "bleujin").put("address", "seoul").getSession().commit() ;
		
		Map<String, ? extends Object> map = MapUtil.chainKeyMap().put("name", "hero").put("age", 20).toMap() ;
		RemoteSession session = super.remoteLogin() ;
		
		session.createQuery().eq("name", "bleujin").overwriteOne(map) ;

		Node found = session.createQuery().id(savedNode.getIdentifier()).findOne();
		
		assertEquals("hero", found.getString("name")) ;
		assertTrue(found.getString("greeting") == null) ;
		assertTrue(found.getString("address") == null) ;
		assertEquals(20, found.getAsInt("age")) ;
	}

	
	public void testUpdateOne() throws Exception {
		Session cs = super.confirmLogin(RemoteTestWorkspaceName) ;
		cs.createQuery().remove() ;
		Node savedNode = cs.newNode() ;
		savedNode.put("name", "bleujin").put("age", 20).getSession().commit() ;
		
		Map<String, ? extends Object> map = MapUtil.chainKeyMap().put("fname", "hero").put("age", 30).toMap() ;
		RemoteSession session = super.remoteLogin() ;
		session.createQuery().eq("age", 20).updateOne(map) ;
		
		Node found = session.createQuery().id(savedNode.getIdentifier()).findOne();
		assertEquals("hero", found.getString("fname")) ;
		assertTrue(found.getString("hero") == null) ;
		assertEquals(30, found.getAsInt("age")) ;
	}

	
	
	public void xtestInc() throws Exception {
		Session cs = super.confirmLogin(RemoteTestWorkspaceName) ;
		cs.createQuery().remove() ;
		Node savedNode = cs.newNode() ;
		savedNode.put("name", "bleujin").put("age", 20).getSession().commit() ;
		
		RemoteSession session = super.remoteLogin() ;
		NodeResult nr = session.createQuery().eq("name", "bleujin").increase("age") ;
		assertEquals(21, session.createQuery().findOne().get("age")) ;
	}
	
	

	
	
	
	

}
