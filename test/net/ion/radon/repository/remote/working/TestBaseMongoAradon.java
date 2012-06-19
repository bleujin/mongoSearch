package net.ion.radon.repository.remote.working;

import junit.framework.TestCase;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.SessionQuery;

public class TestBaseMongoAradon extends TestCase {

	protected final String WORKSPACE_NAME = "abcd";
	protected Session session ;
	protected RepositoryCentral rc ;
	
	protected void setUp() throws Exception {
		super.setUp();
		
		rc = RepositoryCentral.create("61.250.201.78", 27017) ;
		session = rc.testLogin(WORKSPACE_NAME) ;
		session.dropWorkspace();
		session.clear() ;
		session.changeWorkspace(WORKSPACE_NAME) ;
	}
	
	public SessionQuery createQuery(){
		return session.createQuery() ;
	}

}
