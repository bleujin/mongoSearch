package net.ion.radon.repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import net.ion.radon.aclient.ListenableFuture;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.remote.RemoteClient;
import net.ion.radon.repository.remote.body.MergeResponse;
import net.ion.radon.repository.remote.body.QueryBody;
import net.ion.radon.repository.remote.body.QueryResponse;

import com.mongodb.DBObject;

public class RemoteWorkspace extends Workspace {

	private RemoteClient client;
	private String wname;

	private RemoteWorkspace(RemoteClient aclient, String wname) {
		this.client = aclient;
		this.wname = wname;
	}

	public static RemoteWorkspace load(RemoteClient aclient, String wname, WorkspaceOption option) {
		return new RemoteWorkspace(aclient, wname);
	}

	public NodeCursor find(Session session, PropertyQuery iquery, Columns columns) {
		NodeCursor result = client.findRequest(wname, QueryBody.create(iquery, columns)).toNodeCursor(session);
		session.setAttribute(Explain.class.getCanonicalName(), result.explain());
		return result;
	}

	public NodeCursor findDetail(Session session, QueryBody qbody) {

		NodeCursor result = client.findRequest(wname, qbody).toNodeCursor(session);
		session.setAttribute(Explain.class.getCanonicalName(), result.explain());
		return result;
	}

	public String getName() {
		return wname;
	}

	public String toString() {
		return String.format("RemoteWorkspace[%s]", wname);
	}

	protected int mergeNodes(Session session, Map<String, Node> modified) {
		try {
			ListenableFuture<MergeResponse> future = client.merge(session, wname, new HashMap<String, Node>(modified));

			int result = future.get().size();
			return result;
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}

	public NodeResult remove(Session session, PropertyQuery query) {
		Future<MergeResponse> response = client.remove(session, wname, query);

		return NodeResult.NULL;
	}

	public void drop() {
		try {
			Future<MergeResponse> response = client.dropWorkspace(wname);
			response.get();
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public NodeResult updateNode(Session session, PropertyQuery query, DBObject values, boolean upset, boolean multi) {
		client.update(session, wname, query, values, upset, multi);
		return NodeResult.NULL;
	}

	@Override
	protected NodeResult findAndOverwrite(Session session, PropertyQuery query, Map<String, ?> props) {
		client.findAndOverwrite(session, wname, query, new HashMap(props));

		return NodeResult.NULL;
	}

	@Override
	public void makeIndex(IPropertyFamily props, String indexName, boolean unique) {
		client.makeIndex(wname, props, indexName, unique);
	}

	@Override
	public List<NodeObject> getIndexInfo() {
		return client.viewIndexInfo(wname);
	}

	@Override
	protected NodeCursor mapreduce(Session session, String mapFunction, String reduceFunction, String finalFunction, CommandOption options, PropertyQuery condition) {
		try {
			ListenableFuture<QueryResponse> qr = client.mapreduce(wname, condition, options, mapFunction, reduceFunction, finalFunction);
			return qr.get().toNodeCursor(session);
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected List<Node> group(Session session, IPropertyFamily keys, PropertyQuery condition, IPropertyFamily initial, String reduce) {
		try {
			QueryResponse qr = client.group(wname, keys, condition, initial, reduce).get();
			return qr.toNodeCursor(session).toList(PageBean.ALL);
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}

	}

}
