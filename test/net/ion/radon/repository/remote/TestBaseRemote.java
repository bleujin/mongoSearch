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
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestBaseRemote extends TestCase {

	private Aradon aradon;
	private RemoteRepositoryCentral rrc;
	protected final static String RemoteTestWorkspaceName = "rwname";

	private RepositoryCentral rc;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.rc = RepositoryCentral.testCreate();

		this.aradon = new Aradon();
		RemoteClient.attachSection(aradon, rc);
		this.aradon.startServer(9000);
		// AradonClient ac = AradonClientFactory.create("http://localhost:9000") ;

		// AradonClient ac = AradonClientFactory.create(aradon) ;
		this.rrc = RemoteRepositoryCentral.create("http://localhost:9000");
	}

	@Override
	protected void tearDown() throws Exception {
		this.aradon.stop();
		rrc.close();
		super.tearDown();
	}

	public static Test createSuite(Class<? extends TestCase> clz) {
		TestSetup setup = new TestSetup(new TestSuite(clz)) {
			private RepositoryCentral rc ;
			private Aradon aradon ;
			private RemoteRepositoryCentral rrc ;
			protected void setUp() throws Exception {
				rc = RepositoryCentral.testCreate();

				aradon = new Aradon();
				RemoteClient.attachSection(aradon, rc);
				aradon.startServer(9000);
				rrc = RemoteRepositoryCentral.create("http://localhost:9000");
			}
			
			protected void tearDown() throws Exception{
				aradon.stop();
				rrc.close();
			}
		};
		return setup;
	}


	public RemoteSession remoteLogin() {
		return rrc.login(RemoteTestWorkspaceName);
	}

	protected Session confirmLogin(String wsName) {
		return rc.login(wsName);
	}

}
