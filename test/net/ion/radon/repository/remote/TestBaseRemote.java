package net.ion.radon.repository.remote;

import net.ion.radon.client.AradonClient;
import net.ion.radon.client.AradonClientFactory;
import net.ion.radon.core.Aradon;
import net.ion.radon.repository.RemoteRepositoryCentral;
import net.ion.radon.repository.RemoteSession;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.remote.DeleteLet;
import net.ion.radon.repository.remote.FindQueryLet;
import net.ion.radon.repository.remote.MergeLet;
import net.ion.radon.repository.remote.WorkspaceLet;
import net.ion.radon.util.AradonTester;
import junit.framework.TestCase;

public class TestBaseRemote extends TestCase {

	
	private Aradon aradon ;
	private RemoteRepositoryCentral rrc ;
	protected final static String RemoteTestWorkspaceName = "rwname";
	
	private RepositoryCentral rc ;
	@Override protected void setUp() throws Exception {
		super.setUp();
		this.rc = RepositoryCentral.testCreate() ;
		
		
		this.aradon = new Aradon() ;
		RemoteClient.attachSection(aradon, rc) ;
		this.aradon.startServer(9000) ;
//		AradonClient ac = AradonClientFactory.create("http://localhost:9000") ;
		
//		AradonClient ac = AradonClientFactory.create(aradon) ;
		this.rrc = RemoteRepositoryCentral.create("http://localhost:9000") ;
	}
	
	@Override
	protected void tearDown() throws Exception {
		this.aradon.stop() ;
		rrc.close() ;
		super.tearDown();
	}
	

	public RemoteSession remoteLogin(){
		return rrc.login(RemoteTestWorkspaceName) ;
	}
	
	protected Session confirmLogin(String wsName){
		return rc.login(wsName) ;
	}


}
