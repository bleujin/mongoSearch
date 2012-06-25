package net.ion.radon.repository.remote.rest;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ion.framework.util.ListUtil;
import net.ion.framework.util.StringUtil;
import net.ion.radon.core.PageBean;
import net.ion.radon.core.EnumClass.IFormat;
import net.ion.radon.repository.MergeQuery;
import net.ion.radon.repository.Node;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.SessionQuery;
import net.ion.radon.repository.TempNode;
import net.ion.radon.repository.remote.RepositoryResource;

import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;

public class RestLet extends RepositoryResource {

	// /workspace/agroup/gid.format

	@Get
	public Representation viewNode() {
		
		PageBean page = getInnerRequest().getAradonPage() ;
		SessionQuery query = createQuery();
		
		List<Node> found = query.find(page) ;
		return ListNodeRepresentation.toRepresentation(found, getFormat()) ;
	}

	private SessionQuery createQuery() {
		Session session = login();
		SessionQuery query ;
		if (StringUtil.isBlank(getAradonGId())){
			query =  session.createQuery().aradonGroup(getAradonGroup()) ;
		} else {
			query = session.createQuery().aradonGroupId(getAradonGroup(), getAradonGId()) ; 
		}
		return query;
	}
	
	private String getAradonGroup(){
		return getInnerRequest().getAttribute("agroup");
	}
	
	private String getAradonGId(){
		return getInnerRequest().getAttribute("gid");
	}
	private IFormat getFormat(){
		return IFormat.valueOf(getInnerRequest().getAttribute("format")) ;
	}

	
	@Post
	public Representation putNode(){
		Session session = login();
		
		TempNode tempNode = session.tempNode() ;
		Map<String, Object> params = getInnerRequest().getFormParameter() ;
		for (Entry<String, Object> entry : params.entrySet()) {
			tempNode.put(entry.getKey(), entry.getValue()) ;
		}
		session.merge(MergeQuery.createByAradon(getAradonGroup(), getAradonGId()), tempNode) ;
		return ListNodeRepresentation.toRepresentation(ListUtil.toList(tempNode), getFormat()) ;
	}

	
	@Delete
	public Representation deleteNode(){
		return new StringRepresentation(String.valueOf(createQuery().remove()));
	}

}
