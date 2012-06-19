package net.ion.radon.repository.remote;

import java.util.List;

import net.ion.framework.util.ListUtil;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.IPropertyFamily;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeCursor;
import net.ion.radon.repository.NodeImpl;
import net.ion.radon.repository.PropertyQuery;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.remote.body.GroupBody;
import net.ion.radon.repository.remote.body.MapReduceBody;
import net.ion.radon.repository.remote.body.QueryBody;
import net.ion.radon.repository.remote.body.QueryResponse;

import org.restlet.resource.Post;
import org.restlet.resource.Put;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class MapReduceLet extends RepositoryResource{

	
	@Post
	public QueryResponse mapreduce(MapReduceBody body){
		Session session = login() ;
		
		NodeCursor nc = session.createQuery(body.getQuery()).mapreduce(body.mapFn(), body.reduceFn(), body.finalFn(), body.options());

		List<Node> nodes = nc.toList(PageBean.ALL) ; 
		return QueryResponse.create(body.getQuery(), nodes, nc.explain()) ;
	}
	
	@Put
	public QueryResponse group(GroupBody body) {

		Session session = login() ;

		NodeCursor nc = session.createQuery(body.condition()).group(body.keys(), body.initial(), body.reduceFn()) ;
		List<Node> nodes = nc.toList(PageBean.ALL) ; 
		return QueryResponse.create(body.condition(), nodes, nc.explain()) ;
	}

}
