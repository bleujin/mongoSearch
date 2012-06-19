package net.ion.radon.repository.remote;

import net.ion.radon.repository.MergeQuery;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.NodeResult;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.TempNode;
import net.ion.radon.repository.remote.body.MergeBody;
import net.ion.radon.repository.remote.body.MergeResponse;
import net.ion.radon.repository.remote.body.UpdateBody;

import org.restlet.resource.Post;
import org.restlet.resource.Put;

public class MergeLet extends RepositoryResource{


	@Post
	public MergeResponse merge(MergeBody body){
		Session session = login() ;
		
		for (Node node : body.getNodes()) {
			TempNode tnode = node.toTemp(session);
			session.getCurrentWorkspace().merge(session, MergeQuery.createById(node.getIdentifier()), tnode) ;
		}
		
		return MergeResponse.create(body.getNodes().size()) ;
	}
	
	
	@Put
	public MergeResponse update(UpdateBody body){
		
		Session session = login() ;
		NodeResult nr = session.getCurrentWorkspace().updateNode(session, body.getQuery(), body.getValues(), body.upset(), body.multi()) ;
		
		return MergeResponse.create(1) ;	
	}
	
}
