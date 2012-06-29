package net.ion.radon.repository.search;

import junit.framework.TestSuite;

public class TestAllSearch extends TestSuite {

	public static TestSuite suite(){
		TestSuite suite = new TestSuite("Test All Search") ;
		
		suite.addTestSuite(TestDocument.class) ;
		suite.addTestSuite(TestSearchFind.class) ;
		suite.addTestSuite(TestSearchUpdate.class) ;
		suite.addTestSuite(TestSearchUpdateChain.class) ;
		suite.addTestSuite(TestSearchSession.class) ;
		suite.addTestSuite(TestSearchWorkspace.class) ;
		
		
		// suite.addTestSuite(TestBigIndex.class) ;
		
		return suite ;
	}
}
