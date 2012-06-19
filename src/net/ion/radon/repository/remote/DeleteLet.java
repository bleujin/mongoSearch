package net.ion.radon.repository.remote;

import net.ion.radon.repository.Session;
import net.ion.radon.repository.remote.body.MergeResponse;
import net.ion.radon.repository.remote.body.QueryBody;

import org.restlet.resource.Delete;
import org.restlet.resource.Post;

public class DeleteLet extends RepositoryResource{

	@Post
	public MergeResponse deleteNode(QueryBody body){
		Session session = login();

		int result = session.createQuery(body.getQuery()).remove() ;
		return MergeResponse.create(result) ;
	}
	
	@Delete
	public MergeResponse dropWorkspace(){
		Session session = login();
		session.dropWorkspace() ;
		return MergeResponse.create(1) ;
	}
	
}
