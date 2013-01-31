package net.ion.radon.repository.search;

import junit.framework.TestCase;
import net.ion.nsearcher.common.MyDocument;
import net.ion.radon.repository.SearchRepositoryCentral;
import net.ion.radon.repository.SearchSession;

public class TestSearchFirst extends TestCase{

	protected SearchSession session ;
	@Override
	protected void setUp() throws Exception {
		SearchRepositoryCentral rc = SearchRepositoryCentral.testCreate() ;
		session = rc.login("search", "mywork") ;
	}
	
	
	public void testFirst() throws Exception {
		session.dropWorkspace() ;
		
		session.newNode().put("name", "bleujin").put("explain", "ÅÂ±Ø±â°¡ ¹Ù¶÷¿¡ ÆÞ·°ÀÔ´Ï´Ù.").getSession().commit() ;
		
		assertEquals(1, session.createQuery().eq("name", "bleujin").find().count()) ;
		assertEquals(1, session.createSearchQuery().term("explain", "ÅÂ±Ø±â").find().getTotalCount()) ;
		MyDocument found = session.createSearchQuery().term("explain", "ÅÂ±Ø±â").findOne();
		assertEquals("bleujin", found.get("name")) ;
	}
	
	
	
}
