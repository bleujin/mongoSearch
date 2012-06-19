package net.ion.radon.repository.remote.working;

import java.util.List;
import java.util.Map;

import net.ion.framework.parse.gson.JsonObject;
import net.ion.framework.rest.HTMLFormater;
import net.ion.framework.rest.IRequest;
import net.ion.framework.rest.IResponse;
import net.ion.framework.rest.JSONFormater;
import net.ion.framework.rest.XMLFormater;
import net.ion.framework.util.Debug;
import net.ion.framework.util.MapUtil;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.Node;

public class TestFormat extends TestBaseMongoAradon{

	public void testFormater() throws Exception {
		session.createQuery().updateChain().put("name", "bleujin").inlist("friend", MapUtil.chainKeyMap().put("age", 20).put("name", "hm.")).inlist("friend", MapUtil.stringMap("age", 30)).merge();
		Node node = session.createQuery().findOne();

		Debug.line(JsonObject.fromObject(node)) ;
		
		// Debug.line(msg.get("result.nodes"), msg.get("result.nodes").getClass()) ;
	}
	
	public void testSpeed() throws Exception {
		for (int i = 0; i < 1500; i++) {
			session.createQuery().eq("id", i).updateChain().put("name", "bleujin").inlist("friend", MapUtil.chainKeyMap().put("age", 20).put("name", "hm.")).inlist("friend", MapUtil.stringMap("age", 30)).merge();
		}
		
		List<Map<String, ? extends Object>> datas =  session.createQuery().find().toMapList(PageBean.ALL);

		long start = System.currentTimeMillis() ;
		new HTMLFormater().toRepresentation(IRequest.EMPTY_REQUEST, datas, IResponse.EMPTY_RESPONSE) ;
		assertEquals(true, System.currentTimeMillis() - start < 1000) ;

		start = System.currentTimeMillis() ;
		new XMLFormater().toRepresentation(IRequest.EMPTY_REQUEST, datas, IResponse.EMPTY_RESPONSE) ;
		assertEquals(true, System.currentTimeMillis() - start < 1000) ;

		start = System.currentTimeMillis() ;
		new JSONFormater().toRepresentation(IRequest.EMPTY_REQUEST, datas, IResponse.EMPTY_RESPONSE) ;
		assertEquals(true, System.currentTimeMillis() - start < 1000) ;

	}
}
