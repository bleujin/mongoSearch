mongoSearch
===========

remote mongo + search mongo


	Search Mongo

	public class TestSearchFirst extends TestCase{

	protected SearchSession session ;
	@Override
	protected void setUp() throws Exception {
		SearchRepositoryCentral rc = new SearchRepositoryCentral(new Mongo("61.250.201.78"), "test", null, null, CentralConfig.newRam().build()) ;
		session = rc.login("search", "mywork") ;
	}
	
	
	public void testFirst() throws Exception {
		session.dropWorkspace() ;
		
		session.newNode().put("name", "bleujin").put("explain", "�±رⰡ �ٶ��� �޷��Դϴ�.").getSession().commit() ;
		
		assertEquals(1, session.createQuery().eq("name", "bleujin").find().count()) ;
		assertEquals(1, session.createSearchQuery().term("explain", "�±ر�").find().getTotalCount()) ;
		MyDocument found = session.createSearchQuery().term("explain", "�±ر�").findOne();
		assertEquals("bleujin", found.get("name")) ;
	}
	
	
	
}