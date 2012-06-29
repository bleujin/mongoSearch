package net.ion.radon.repository.remote.rest;

import java.util.Date;

import junit.framework.TestCase;
import net.ion.framework.util.Debug;
import net.ion.radon.repository.InListNode;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeCursor;
import net.ion.radon.repository.RemoteRepositoryCentral;
import net.ion.radon.repository.RemoteSession;

public class TestAmazonLet extends TestCase {


	private RemoteRepositoryCentral rc ; 
	private RemoteSession session ;
	protected void setUp() throws Exception {
		super.setUp();
		this.rc = RemoteRepositoryCentral.create("http://54.248.108.179:9000") ;
		this.session = rc.login("stest") ;
	}
	
	protected void tearDown() throws Exception {
		rc.close() ;
		super.tearDown();
	}
	
	public void testCreateNode() throws Exception {
		session.dropWorkspace() ;
		session.newNode().put("name", "bleujin").put("age", 20).put("birthday", new Date()).getSession().commit() ;
		
		NodeCursor nc = session.createQuery().eq("name", "bleujin").find() ;
		Node found = nc.next() ;
		
		assertEquals(20, found.getAsInt("age")) ;
		Debug.line(found) ;
	}
	
	public void testUpdateNode() throws Exception {
		session.dropWorkspace() ;
		session.newNode().put("name", "bleujin").put("age", 20).put("birthday", new Date()).getSession().commit() ;
		
		session.createQuery().eq("name", "bleujin").updateChain().inc("age", 1).put("address", "seoul").update() ;
		Node found = session.createQuery().eq("name", "bleujin").findOne() ;
		
		assertEquals(21, found.getAsInt("age")) ;
		Debug.line(found) ;
	}
	
	public void testAppend() throws Exception {
		session.dropWorkspace() ;
		session.newNode().put("name", "bleujin").append("address", "seoul").append("address", "busan").getSession().commit() ;
		
		InListNode address = session.createQuery().eq("name", "bleujin").findOne().inlist("address") ;
		assertEquals("seoul", address.get(0)) ;
		assertEquals("busan", address.get(1)) ;
	}
	
	
}
