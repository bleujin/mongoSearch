package net.ion.nsearcher.directory;

import net.ion.framework.util.Debug;
import net.ion.radon.aclient.NewClient;
import net.ion.radon.aclient.Response;
import junit.framework.TestCase;

public class TestElastic extends TestCase {

	public void testIndex() throws Exception {
		NewClient nc = NewClient.create() ;
		
		
		Debug.line(nc.preparePut("http://61.250.201.157:9200/twitter/user/kimchy").setBody("{\"user\": \"Shay Banon\"}").execute().get().getTextBody()) ;
		Debug.line(nc.preparePut("http://61.250.201.157:9200/twitter/tweet/1").setBody("{\"user\": \"kimchy\",\"postDate\": \"2009-11-15T13:12:00\",\"message\": \"Trying out Elastic Search, so far so good?\"}}").execute().get().getTextBody()) ;
		Debug.line(nc.preparePut("http://61.250.201.157:9200/twitter/tweet/2").setBody("{\"user\": \"kimchy\",\"postDate\": \"2009-11-15T14:12:12\",\"message\": \"Another tweet, will it be indexed?\"}}").execute().get().getTextBody()) ;

		nc.close() ;
	}
	
	public void testDelete() throws Exception {
		NewClient nc = NewClient.create() ;
		
		
		Debug.line(nc.prepareDelete("http://61.250.201.157:9200/twitter/tweet/_search").execute().get().getTextBody()) ;

		nc.close() ;
	}
	
	public void testSearch() throws Exception {
		NewClient nc = NewClient.create() ;
		
		Response res = nc.preparePost("http://61.250.201.157:9200/twitter/tweet/_search?pretty=true").setBody("{ \"query\" : { matchAll :{}} }").execute().get() ;
		
		Debug.line(res.getTextBody()) ;
		nc.close() ;
	}
}
