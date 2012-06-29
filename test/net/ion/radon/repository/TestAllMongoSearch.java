package net.ion.radon.repository;

import junit.framework.TestSuite;
import net.ion.radon.repository.remote.TestAllRemote;
import net.ion.radon.repository.search.TestAllSearch;
import net.ion.radon.repository.search.TestDocument;
import net.ion.radon.repository.search.TestSearchFind;
import net.ion.radon.repository.search.TestSearchUpdate;
import net.ion.radon.repository.search.TestSearchUpdateChain;
import net.ion.radon.repository.search.TestSearchWorkspace;

public class TestAllMongoSearch extends TestSuite {

	public static TestSuite suite(){
		TestSuite suite = new TestSuite("Test All MongoSearch") ;
		
//		suite.addTest(TestAllRemote.suite()) ;
		suite.addTest(TestAllSearch.suite()) ;

		
		return suite ;
	}
}

