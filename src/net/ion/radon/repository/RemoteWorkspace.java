package net.ion.radon.repository;

import java.util.List;
import java.util.Map;

import net.ion.framework.util.ListUtil;
import net.ion.framework.util.StringUtil;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.remote.RemoteClient;
import net.ion.radon.repository.remote.body.MergeResponse;
import net.ion.radon.repository.remote.body.QueryBody;
import net.ion.radon.repository.remote.body.QueryResponse;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MapReduceCommand.OutputType;

public class RemoteWorkspace extends Workspace{

	private RemoteClient client ;
	private String wname ;
	private RemoteWorkspace(RemoteClient aclient, String wname){
		this.client = aclient ;
		this.wname = wname ;
	}

	public static RemoteWorkspace load(RemoteClient aclient, String wname, WorkspaceOption option) {
		return new RemoteWorkspace(aclient, wname);
	}
	
	public NodeCursor find(Session session, PropertyQuery iquery, Columns columns) {
		NodeCursor result = client.findRequest(wname, QueryBody.create(iquery, columns)).toNodeCursor(session);
		session.setAttribute(Explain.class.getCanonicalName(), result.explain()) ;
		return result;
	}
	
	public NodeCursor findDetail(Session session, QueryBody qbody){
		
		NodeCursor result = client.findRequest(wname, qbody).toNodeCursor(session);
		session.setAttribute(Explain.class.getCanonicalName(), result.explain()) ;
		return result;
	}

	public String getName() {
		return wname;
	}
	
	public String toString(){
		return String.format("RemoteWorkspace[%s]", wname) ;
	}
	
	protected int mergeNodes(Session session, Map<String, Node> modified) {
		int result = client.merge(session, wname, modified).size() ;
		return result ;
	}
	
	public NodeResult remove(Session session, PropertyQuery query) {
		MergeResponse response = client.remove(session, wname, query) ;
		
		return NodeResult.NULL;
	}

	public void drop() {
		MergeResponse response = client.dropWorkspace(wname) ;
	}
	
	@Override
	public NodeResult updateNode(Session session, PropertyQuery query, DBObject values, boolean upset, boolean multi) {
		client.update(session, wname, query, values, upset, multi) ;
		return NodeResult.NULL ;
	}
	
	@Override
	protected NodeResult findAndOverwrite(Session session, PropertyQuery query, Map<String, ?> props) {
		client.findAndOverwrite(session, wname, query, props) ;
		
		return NodeResult.NULL ;
	}

	@Override
	public void makeIndex(IPropertyFamily props, String indexName, boolean unique) {
		client.makeIndex(wname, props, indexName, unique) ;
	}
	
	@Override 
	public List<NodeObject> getIndexInfo(){
		return client.viewIndexInfo(wname) ;
	} 

	
	@Override
	protected NodeCursor mapreduce(Session session, String mapFunction, String reduceFunction, String finalFunction, CommandOption options, PropertyQuery condition) {
		
		QueryResponse qr  = client.mapreduce(wname, condition, options, mapFunction, reduceFunction, finalFunction) ;
		return qr.toNodeCursor(session) ;
	}
	
	@Override
	protected List<Node> group(Session session, IPropertyFamily keys, PropertyQuery condition, IPropertyFamily initial, String reduce) {
		
		QueryResponse qr = client.group(wname, keys, condition, initial, reduce) ;
		
		return qr.toNodeCursor(session).toList(PageBean.ALL) ; 

	}

	
}
