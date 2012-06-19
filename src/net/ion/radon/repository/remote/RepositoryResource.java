package net.ion.radon.repository.remote;

import net.ion.radon.core.let.AbstractServerResource;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;

public class RepositoryResource extends AbstractServerResource {

	protected Session login(){
		RepositoryCentral rc =  getContext().getAttributeObject(RepositoryCentral.class.getCanonicalName(), RepositoryCentral.class) ;
		String wname = getInnerRequest().getAttribute("wname");
		Session session = rc.testLogin(wname) ;
		return session ;
	}
}
