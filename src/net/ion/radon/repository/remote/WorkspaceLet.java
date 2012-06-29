package net.ion.radon.repository.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.plaf.ListUI;

import org.restlet.resource.Get;
import org.restlet.resource.Post;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.MapUtil;
import net.ion.radon.repository.NodeObject;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.remote.body.MergeResponse;
import net.ion.radon.repository.remote.body.WorkspaceBody;

public class WorkspaceLet extends RepositoryResource {

	@Get
	public List<NodeObject> viewIndexInfo(){
		Session session = login() ;
		
		List<NodeObject> result = session.getCurrentWorkspace().getIndexInfo() ;
		//if (result.size() == 0) return new ArrayList<NodeObject>() ;
//		return (ArrayList<NodeObject>) result ;
		
		List<NodeObject> list = ListUtil.newList() ;
		for (NodeObject no : result) {
			Map<String, ?> map = no.getDBObject().toMap() ;
			Map newMap = MapUtil.newMap() ;
			for (Entry entry : map.entrySet()) {
				if (! entry.getValue().getClass().equals(BasicDBObject.class)){
					newMap.put(entry.getKey(), entry.getValue()) ;
				} else {
					newMap.put(entry.getKey(), entry.getValue().toString()) ;
//					Debug.line(entry.getKey(), entry.getValue(), entry.getValue().getClass(), entry.getValue().getClass() ) ;
				}
			}
			list.add(NodeObject.load(newMap)) ;
		}
		return list ;
		
	}
	
	@Post
	public MergeResponse makeIndex(WorkspaceBody body){
		Session session = login() ;
		
		session.getCurrentWorkspace().makeIndex(body.getProp(), body.getIndexName(), body.isUnique()) ;
		return MergeResponse.create(1) ;
	}
}
