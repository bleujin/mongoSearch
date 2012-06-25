package net.ion.radon.repository.remote;

import net.ion.framework.util.Debug;
import net.ion.radon.aclient.NewClient;
import net.ion.radon.aclient.Response;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.RemoteRepositoryCentral;
import net.ion.radon.repository.RemoteSession;
import junit.framework.TestCase;

public class TestRemotePlugIn extends TestCase {

	
	public void testConnect() throws Exception {
		Response response = NewClient.create().prepareGet("http://54.248.108.179:9000").execute().get() ;
		Debug.line(response.getTextBody()) ;
	}
	
	public void testLogin() throws Exception {
		RemoteRepositoryCentral rrc = RemoteRepositoryCentral.create("http://54.248.108.179:9000");
		
		RemoteSession session =  rrc.login("test") ;
		session.newNode().put("name", "bleujin").getSession().commit() ;
		
		session.createQuery().find().debugPrint(PageBean.ALL) ;
	}
}
