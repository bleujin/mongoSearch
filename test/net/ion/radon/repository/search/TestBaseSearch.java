package net.ion.radon.repository.search;

import junit.framework.TestCase;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.SearchRepositoryCentral;
import net.ion.radon.repository.SearchSession;
import net.ion.radon.repository.Session;

public class TestBaseSearch extends TestCase{


	protected SearchRepositoryCentral rc ;
	protected Session old ;
	protected SearchSession session ;
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		rc = SearchRepositoryCentral.testCreate() ;
		session = rc.login("search", "mywork") ;
		
		RepositoryCentral mrc = RepositoryCentral.testCreate() ;
		old = mrc.login("search", "mywork") ;
	}
	
	
}
