package net.ion.radon.repository.remote;

import java.util.List;

import org.restlet.resource.Get;
import org.restlet.resource.Post;

import net.ion.radon.repository.NodeObject;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.remote.body.MergeResponse;
import net.ion.radon.repository.remote.body.WorkspaceBody;

public class WorkspaceLet extends RepositoryResource {

	@Get
	public List<NodeObject> viewIndexInfo(){
		Session session = login() ;
		
		List<NodeObject> result = session.getCurrentWorkspace().getIndexInfo() ;
		return result ;
	}
	
	@Post
	public MergeResponse makeIndex(WorkspaceBody body){
		Session session = login() ;
		
		session.getCurrentWorkspace().makeIndex(body.getProp(), body.getIndexName(), body.isUnique()) ;
		return MergeResponse.create(1) ;
	}
}
