package net.ion.radon.repository.remote;

import junit.framework.TestSuite;
import net.ion.radon.repository.search.TestDocument;
import net.ion.radon.repository.search.TestSearchFind;
import net.ion.radon.repository.search.TestSearchUpdate;
import net.ion.radon.repository.search.TestSearchUpdateChain;
import net.ion.radon.repository.search.TestSearchWorkspace;

public class TestAllRemote extends TestSuite {

	public static TestSuite suite(){
		TestSuite suite = new TestSuite("Test All Remote") ;
		
		suite.addTestSuite(TestFindQuery.class) ;
		suite.addTestSuite(TestRemoteFirst.class) ;
		suite.addTestSuite(TestRemoteUpdate.class) ;
		suite.addTestSuite(TestRemoteUpdateChain.class) ;
		suite.addTestSuite(TestRemoteWorkspace.class) ;
		suite.addTestSuite(TestMapReduce.class) ;
		
		return suite ;
	}
}
