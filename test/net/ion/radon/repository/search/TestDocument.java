package net.ion.radon.repository.search;

import net.ion.nsearcher.common.MyDocument;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeConstants;

public class TestDocument extends TestBaseSearch{

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		session.createQuery().remove();
	}
	

	public void testCreateDocument() throws Exception {
		session.newNode().put("name", "bleujin").put("age", 20).put("index", 0).getSession().commit() ;
		
		session.waitForFlushed() ;
		MyDocument doc = session.createSearchQuery().find().getDocument().get(0) ;
		
		assertEquals("bleujin", doc.get("name")) ;
		assertEquals("20", doc.get("age")) ;
		assertEquals("0", doc.get("index")) ;
		
		assertEquals(20L, doc.getAsLong("age")) ;
		assertEquals(0L, doc.getAsLong("index")) ;
	}
	
	public void testInner() throws Exception {
		session.newNode()
			.inner("name").put("fname", "bleu").put("lname", "jin").put("index", 0).getParent().getSession().commit() ;

		session.waitForFlushed() ;
		MyDocument doc = session.createSearchQuery().find().getDocument().get(0) ;
		
		assertEquals("bleu", doc.get("name.fname")) ;
	}
	
	public void testNodeInfo() throws Exception {
		Node node = session.newNode() ;
		node.setAradonId("emp", "bleujin").inner("name").put("fname", "bleu").put("lname", "jin").put("index", 0) ;
		session.commit() ;

		session.waitForFlushed() ;
		MyDocument doc = session.createSearchQuery().find().getDocument().get(0) ;
		assertEquals(session.getCurrentWorkspaceName(), doc.get(NodeConstants.WSNAME)) ;
		assertEquals(node.getAradonId().getGroup(), doc.get(NodeConstants.ARADON_GROUP)) ;
		assertEquals(node.getAradonId().getUid(), doc.get(NodeConstants.ARADON_UID)) ;
	}
	
	
}
