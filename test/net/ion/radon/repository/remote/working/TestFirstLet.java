package net.ion.radon.repository.remote.working;

import org.restlet.resource.Get;

import net.ion.framework.util.Debug;
import net.ion.radon.client.AradonClient;
import net.ion.radon.client.AradonClientFactory;
import net.ion.radon.core.SectionService;
import net.ion.radon.core.let.AbstractServerResource;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeResult;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.TestBaseRepository;
import net.ion.radon.util.AradonTester;

public class TestFirstLet extends TestBaseMongoAradon {

	public void testNewNode() throws Exception {
		Node node = session.newNode().put("name", "bleujin");
		session.commit();

		Node found = session.createQuery().findOne();
		assertEquals(true, session == node.getSession());
		assertEquals(true, node.getSession() == found.getSession());
	}

	public void testLoad() throws Exception {
		AradonTester at = AradonTester.create().register("", "/test", FirstLet.class);

		SectionService ss = at.getAradon().getChildService("");
		RepositoryCentral rc = RepositoryCentral.testCreate();
		ss.getServiceContext().putAttribute(RepositoryCentral.class.getCanonicalName(), rc);
		at.getAradon().startServer(9000);

		AradonClient ac = AradonClientFactory.create("http://localhost:9000");
		Node foundNode = ac.createSerialRequest("/test").get(Node.class);

		assertEquals(true, foundNode != null);
		assertEquals("bleujin", foundNode.getString("name"));
		try {
			foundNode.getSession();
			fail();
		} catch (IllegalStateException expect) {
		}
		at.getAradon().stop();
	}

}

class FirstLet extends AbstractServerResource {

	@Get
	public Node getNode(){
		
		RepositoryCentral rc = getContext().getAttributeObject(RepositoryCentral.class.getCanonicalName(), RepositoryCentral.class) ;
		Session session = rc.testLogin("abcd") ;
		
		Node node = session.newNode().put("name", "bleujin") ;
		
		Debug.line(session, node.getSession()) ;
		int count = session.commit() ;
		Debug.line(count, session.getModified().size(), session.getAttribute(NodeResult.class.getCanonicalName(), NodeResult.class)) ;

		
		Node result = session.createQuery().findOne();
		return result ;
	}
	
}

