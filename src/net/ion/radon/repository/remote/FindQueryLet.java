package net.ion.radon.repository.remote;

import java.util.List;

import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeCursor;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.remote.body.QueryBody;
import net.ion.radon.repository.remote.body.QueryResponse;

import org.restlet.resource.Post;

public class FindQueryLet extends RepositoryResource{

	
	@Post
	public QueryResponse find(QueryBody body){
		Session session = login() ;
		
		NodeCursor nc = session.createQuery(body.getQuery()).find(body.getColumns()).limit(body.getLimit()).skip(body.getSkip()).sort(body.getSort());
		
		List<Node> nodes = nc.toList(body.getPage()) ; 
		return QueryResponse.create(body.getQuery(), nodes, nc.explain()) ;
	}
}
