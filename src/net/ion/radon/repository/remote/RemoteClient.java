package net.ion.radon.repository.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.ion.framework.util.InstanceCreationException;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class RemoteClient {

	private AradonClient ac ;
	private String sectionName ;
	private RemoteClient(AradonClient ac, String sectionName){
		this.ac = ac ;
		this.sectionName = sectionName ;
	}
	public final static String DefaultSectionName = "plugin.remote.repository" ;
	
	public static RemoteClient create(AradonClient ac){
		return create(ac, DefaultSectionName) ;
	}
	public static RemoteClient create(AradonClient ac, String sectionName){
		return new RemoteClient(ac, sectionName) ;
	}
	
	public QueryResponse findRequest(String wname, QueryBody qbody) {
		ISerialRequest req = ac.createSerialRequest(makePath("query", wname)) ;
		
		return req.post(qbody, QueryResponse.class) ;
	}

	private String makePath(String letName, String wname){
		return "/" + sectionName + "/" + letName + "/" + wname ;
	}

	public MergeResponse remove(Session session, String wname, PropertyQuery query) {
		ISerialRequest req = ac.createSerialRequest(makePath("delete",  wname )) ;
		return req.post(QueryBody.create(query, Columns.ALL), MergeResponse.class) ;
	}
	
	public MergeResponse dropWorkspace(String wname){
		ISerialRequest req = ac.createSerialRequest(makePath("delete", wname)) ;
		return req.delete(MergeResponse.class) ;
	}

	
	public MergeResponse merge(Session session, String wname, Map<String, Node> modified){
		ISerialRequest req = ac.createSerialRequest(makePath("merge",wname)) ;
		
		return req.post(MergeBody.create(new ArrayList(modified.values())), MergeResponse.class) ;
	}


	public MergeResponse update(Session session, String wname, PropertyQuery query, DBObject values, boolean upset, boolean multi) {
		ISerialRequest req = ac.createSerialRequest(makePath("merge", wname)) ;
		
		return req.put(UpdateBody.create(query, values, upset, multi), MergeResponse.class);
	}

	public void findAndOverwrite(Session session, String wname, PropertyQuery query, Map<String, ?> props) {
		update(session, wname, query, new BasicDBObject(props), false, false) ;
	}

	public void makeIndex(String wname, IPropertyFamily props, String indexName, boolean unique) {
		ISerialRequest req = ac.createSerialRequest(makePath("workspace", wname)) ;
		req.post(WorkspaceBody.create(props, indexName, unique), MergeResponse.class) ;
	}

	public List<NodeObject> viewIndexInfo(String wname) {
		ISerialRequest req = ac.createSerialRequest(makePath("workspace", wname)) ;
		return req.get(List.class) ;
	}
	
	public QueryResponse mapreduce(String wname, PropertyQuery query, CommandOption options, String mapFn, String reduceFn, String finalFn){
		ISerialRequest req = ac.createSerialRequest(makePath("mapreduce", wname)) ;
		return req.post(MapReduceBody.create(query, options, mapFn, reduceFn, finalFn), QueryResponse.class) ;
	}

	public QueryResponse group(String wname, IPropertyFamily keys, PropertyQuery condition, IPropertyFamily initial, String reduceFn) {
		ISerialRequest req = ac.createSerialRequest(makePath("mapreduce", wname)) ;
		return req.put(GroupBody.create(keys, condition, initial, reduceFn), QueryResponse.class) ;
	}
	
	
	public final static SectionService attachSection(Aradon aradon, RepositoryCentral rc) throws ConfigurationException, InstanceCreationException, Exception{
		
		return AradonTester.load(aradon)
			.mergeSection(RemoteClient.DefaultSectionName)
			.putAttribute(RepositoryCentral.class.getCanonicalName(), rc)
			.addLet("/query/{wname}", "query", FindQueryLet.class)
			.addLet("/merge/{wname}", "merge", MergeLet.class)
			.addLet("/delete/{wname}", "delete", DeleteLet.class)
			.addLet("/workspace/{wname}", "workspace", WorkspaceLet.class)
			.addLet("/mapreduce/{wname}", "mapreduce", MapReduceLet.class)
			.getAradon().getChildService(RemoteClient.DefaultSectionName) ;
		
	}
	
}
