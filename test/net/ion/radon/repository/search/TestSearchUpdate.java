package net.ion.radon.repository.search;

import java.util.Map;

import net.ion.framework.util.Debug;
import net.ion.framework.util.MapUtil;
import net.ion.nsearcher.common.MyDocument;
import net.ion.radon.repository.MergeQuery;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeResult;
import net.ion.radon.repository.TempNode;

public class TestSearchUpdate extends TestBaseSearch {

	public void testTempNode() throws Exception {
		session.createQuery().remove() ;

		TempNode tnode = session.tempNode() ;
		tnode.put("name", "bleujin").put("age", 20).append("friend", "novision").append("friend", "iihi") ;
		session.merge(MergeQuery.createByAradon("emp", "bleujin"), tnode) ;
		
		session.waitForFlushed() ;
		assertEquals(1, session.createQuery().eq("name", "bleujin").find().count()) ;
		assertEquals(1, session.createSearchQuery().find().totalCount()) ;
	}
	
	public void testRemoveNode() throws Exception {
		session.createQuery().remove() ;
		
		session.newNode().put("name", "bleujin").getSession().commit() ;
		Node found = session.createQuery().eq("name", "bleujin").findOne() ; 
		
		assertEquals(true, found != null) ;
		
		session.remove(found) ;
		session.waitForFlushed() ;

		assertEquals(0, session.createQuery().find().count()) ;
		assertEquals(0, session.createSearchQuery().find().totalCount()) ;
	}
	
	public void testRemoveNode2() throws Exception {
		session.createQuery().remove() ;
		
		session.newNode().put("name", "bleujin").getSession().commit() ;
		Node found = session.createQuery().eq("name", "bleujin").findOne() ; 
		
		assertEquals(true, found != null) ;
		
		session.createQuery().eq("name", "bleujin").remove() ;
		session.waitForFlushed() ;

		assertEquals(0, session.createQuery().find().count()) ;
		assertEquals(0, session.createSearchQuery().find().totalCount()) ;
	}
	
	
	public void testDropWorkspace() throws Exception {
		session.createQuery().remove() ;
		session.newNode().put("name", "bleujin").getSession().commit() ;
		session.waitForFlushed() ;
		
		assertEquals(1, session.createQuery().find().count()) ;
		assertEquals(1, session.createSearchQuery().find().totalCount()) ;

		session.getCurrentWorkspace().drop() ;
		session.waitForFlushed() ;
		
		assertEquals(0, session.createQuery().find().count()) ;
		assertEquals(0, session.createSearchQuery().find().totalCount()) ;
		
	}
	
	public void testOverwriteOne() throws Exception {
		session.createQuery().remove() ;
		Node savedNode = session.newNode() ;
		savedNode.put("name", "bleujin").put("address", "seoul").getSession().commit() ;
		
		Map<String, ? extends Object> map = MapUtil.chainKeyMap().put("name", "hero").put("age", 20).toMap() ;
		session.createQuery().eq("name", "bleujin").overwriteOne(map) ;
		session.waitForFlushed() ;

		
		
		Node found = session.createQuery().id(savedNode.getIdentifier()).findOne();
		
		assertEquals("hero", found.getString("name")) ;
		assertTrue(found.getString("greeting") == null) ;
		assertTrue(found.getString("address") == null) ;
		assertEquals(20, found.getAsInt("age")) ;
		
		MyDocument findDoc = session.createSearchQuery().findOne();
		assertEquals("hero", findDoc.get("name")) ;
		assertEquals(20L, findDoc.getAsLong("age")) ;
		assertEquals(true, findDoc.get("address") == null) ;
	}
	
	public void testUpdateOn() throws Exception {
		session.createQuery().remove() ;
		Node savedNode = session.newNode() ;
		savedNode.put("name", "bleujin").put("age", 20).getSession().commit() ;
		
		Map<String, ? extends Object> map = MapUtil.chainKeyMap().put("fname", "hero").put("age", 30).toMap() ;
		session.createQuery().eq("age", 20).updateOne(map) ;
		session.waitForFlushed() ;

		
		
		Node found = session.createQuery().id(savedNode.getIdentifier()).findOne();
		assertEquals("hero", found.getString("fname")) ;
		assertTrue(found.getString("hero") == null) ;
		assertEquals(30, found.getAsInt("age")) ;
		
		MyDocument findDoc = session.createSearchQuery().findOne();
		Debug.line(findDoc.get("name"), findDoc.get("fname")) ;
		assertEquals("bleujin", findDoc.get("name")) ;
		assertEquals("hero", findDoc.get("fname")) ;
//		assertEquals(20L, findDoc.getAsLong("age")) ;
	}
	
	
	public void testInc() throws Exception {
		session.createQuery().remove() ;
		Node savedNode = session.newNode() ;
		savedNode.put("name", "bleujin").put("age", 20).getSession().commit() ;
		
		NodeResult nr = session.createQuery().eq("name", "bleujin").increase("age") ;
		session.waitForFlushed() ;

		assertEquals(21L, session.createSearchQuery().findOne().getAsLong("age")) ;
	}
	
	
	
	
}
