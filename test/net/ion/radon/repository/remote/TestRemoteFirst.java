package net.ion.radon.repository.remote;

import net.ion.framework.util.Debug;
import net.ion.radon.aclient.ClientConfig;
import net.ion.radon.aclient.NewClient;
import net.ion.radon.aclient.ClientConfig.Builder;
import net.ion.radon.aclient.providers.netty.NettyProvider;
import net.ion.radon.client.AradonClient;
import net.ion.radon.client.AradonClientFactory;
import net.ion.radon.core.Aradon;
import net.ion.radon.core.config.XMLConfig;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.RemoteRepositoryCentral;
import net.ion.radon.repository.RemoteSession;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.remote.DeleteLet;
import net.ion.radon.repository.remote.FindQueryLet;
import net.ion.radon.repository.remote.MergeLet;
import net.ion.radon.repository.remote.body.MergeBody;
import net.ion.radon.util.AradonTester;
import junit.framework.TestCase;

public class TestRemoteFirst extends TestCase{

	
	private Aradon aradon ;
	private RemoteRepositoryCentral rrc ;
	protected final static String RemoteTestWorkspaceName = "rwname";
	
	@Override protected void setUp() throws Exception {
		super.setUp() ;
		RepositoryCentral rc = RepositoryCentral.testCreate() ;
		this.aradon = new Aradon() ;
		RemoteClient.attachSection(aradon, rc) ;
		
		this.aradon.startServer(9000) ;
		this.rrc = RemoteRepositoryCentral.create("http://localhost:9000") ;
	}


	@Override
	protected void tearDown() throws Exception {
		this.aradon.stop() ;
		super.tearDown();
	}


	public void testFirst() throws Exception {
		RemoteSession session = rrc.login(RemoteTestWorkspaceName) ;
		session.dropWorkspace() ;
		session.newNode().put("name", "bleujin").put("age", 20) ;
		session.commit() ;
		
		Node found = session.createQuery().eq("name", "bleujin").findOne() ;
		assertEquals(20, found.get("age")) ;
	}

	
}
