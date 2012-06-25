package net.ion.radon.repository.remote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.ion.framework.util.InstanceCreationException;
import net.ion.radon.aclient.ISerialAsyncRequest;
import net.ion.radon.aclient.ListenableFuture;
import net.ion.radon.aclient.NewClient;
import net.ion.radon.client.AradonClient;
import net.ion.radon.client.ISerialRequest;
import net.ion.radon.core.Aradon;
import net.ion.radon.core.SectionService;
import net.ion.radon.repository.Columns;
import net.ion.radon.repository.CommandOption;
import net.ion.radon.repository.IPropertyFamily;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeObject;
import net.ion.radon.repository.PropertyQuery;
import net.ion.radon.repository.RCentral;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.remote.body.GroupBody;
import net.ion.radon.repository.remote.body.MapReduceBody;
import net.ion.radon.repository.remote.body.MergeBody;
import net.ion.radon.repository.remote.body.MergeResponse;
import net.ion.radon.repository.remote.body.QueryBody;
import net.ion.radon.repository.remote.body.QueryResponse;
import net.ion.radon.repository.remote.body.UpdateBody;
import net.ion.radon.repository.remote.body.WorkspaceBody;
import net.ion.radon.util.AradonTester;

import org.apache.commons.configuration.ConfigurationException;
import org.restlet.data.Method;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class RemoteClient {

	private NewClient ac ;
	private String prePath ;
	private RemoteClient(NewClient ac, String prePath){
		this.ac = ac ;
		this.prePath = prePath ;
	}
	public final static String DefaultSectionName = "plugin.remote.repository" ;
	

	public static RemoteClient create(NewClient ac, String prePath){
		return new RemoteClient(ac, prePath) ;
	}
	
	public QueryResponse findRequest(String wname, QueryBody qbody) {
		try {
			ISerialAsyncRequest req = ac.createSerialRequest(makePath("query", wname)) ;
			ListenableFuture<QueryResponse> future = req.handle(Method.POST, qbody, QueryResponse.class);
			return future.get() ;
		} catch (Throwable e) {
			throw new IllegalStateException(e) ;
		}
	}

	private String makePath(String letName, String wname){
		return prePath + "/" + letName + "/" + wname ;
	}
	
	public ListenableFuture<MergeResponse> remove(Session session, String wname, PropertyQuery query) {
		ISerialAsyncRequest req = ac.createSerialRequest(makePath("delete",  wname )) ;
		return req.post(QueryBody.create(query, Columns.ALL), MergeResponse.class) ;
	}
	
	public ListenableFuture<MergeResponse> dropWorkspace(String wname){
		ISerialAsyncRequest req = ac.createSerialRequest(makePath("delete", wname)) ;
		return req.delete(MergeResponse.class) ;
	}

	
	public ListenableFuture<MergeResponse> merge(Session session, String wname, HashMap<String, Node> modified){
		ISerialAsyncRequest req = ac.createSerialRequest(makePath("merge",wname)) ;
		
		return req.post(MergeBody.create(new ArrayList(modified.values())), MergeResponse.class) ;
	}


	public ListenableFuture<MergeResponse> update(Session session, String wname, PropertyQuery query, DBObject values, boolean upset, boolean multi) {
		ISerialAsyncRequest req = ac.createSerialRequest(makePath("merge", wname)) ;
		
		return req.put(UpdateBody.create(query, values, upset, multi), MergeResponse.class);
	}

	public void findAndOverwrite(Session session, String wname, PropertyQuery query, Map<String, ?> props) {
		update(session, wname, query, new BasicDBObject(props), false, false) ;
	}

	public void makeIndex(String wname, IPropertyFamily props, String indexName, boolean unique) {
		ISerialAsyncRequest req = ac.createSerialRequest(makePath("workspace", wname)) ;
		req.post(WorkspaceBody.create(props, indexName, unique), MergeResponse.class) ;
	}

	public List<NodeObject> viewIndexInfo(String wname) {
		try {
			ISerialAsyncRequest req = ac.createSerialRequest(makePath("workspace", wname)) ;
			return req.get(List.class).get() ;
		} catch (Throwable e) {
			throw new IllegalStateException(e) ;
		}
	}
	
	public ListenableFuture<QueryResponse> mapreduce(String wname, PropertyQuery query, CommandOption options, String mapFn, String reduceFn, String finalFn){
		ISerialAsyncRequest req = ac.createSerialRequest(makePath("mapreduce", wname)) ;
		return req.post(MapReduceBody.create(query, options, mapFn, reduceFn, finalFn), QueryResponse.class) ;
	}

	public ListenableFuture<QueryResponse> group(String wname, IPropertyFamily keys, PropertyQuery condition, IPropertyFamily initial, String reduceFn) {
		ISerialAsyncRequest req = ac.createSerialRequest(makePath("mapreduce", wname)) ;
		return req.put(GroupBody.create(keys, condition, initial, reduceFn), QueryResponse.class) ;
	}
	
	
	public final static SectionService attachSection(Aradon aradon, RCentral rc) throws ConfigurationException, InstanceCreationException, Exception{
		
		return AradonTester.load(aradon)
			.mergeSection(RemoteClient.DefaultSectionName)
			.putAttribute(RCentral.class.getCanonicalName(), rc)
			.addLet("/query/{wname}", "query", FindQueryLet.class)
			.addLet("/merge/{wname}", "merge", MergeLet.class)
			.addLet("/delete/{wname}", "delete", DeleteLet.class)
			.addLet("/workspace/{wname}", "workspace", WorkspaceLet.class)
			.addLet("/mapreduce/{wname}", "mapreduce", MapReduceLet.class)
			.getAradon().getChildService(RemoteClient.DefaultSectionName) ;
		
	}
	
}
