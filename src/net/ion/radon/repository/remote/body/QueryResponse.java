package net.ion.radon.repository.remote.body;

import java.io.Serializable;
import java.util.List;

import net.ion.radon.repository.Explain;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeCursor;
import net.ion.radon.repository.PropertyQuery;
import net.ion.radon.repository.ProxyCursor;
import net.ion.radon.repository.Session;

public class QueryResponse implements Serializable {

	private static final long serialVersionUID = 8584228587988341605L;
	private PropertyQuery query;
	private List<Node> nodes;
	private Explain explain ;

	private QueryResponse(PropertyQuery query, List<Node> nodes, Explain explain) {
		this.query = query;
		this.nodes = nodes;
		this.explain = explain ;
	}

	public NodeCursor toNodeCursor(Session session) {
		return ProxyCursor.create(session, query, nodes, explain);
	}

	public static QueryResponse create(PropertyQuery query, List<Node> nodes, Explain explain) {
		return new QueryResponse(query, nodes, explain);
	}

}
