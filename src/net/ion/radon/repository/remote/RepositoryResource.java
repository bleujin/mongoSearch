package net.ion.radon.repository.remote;

import net.ion.radon.core.let.AbstractServerResource;
import net.ion.radon.repository.RCentral;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;

public class RepositoryResource extends AbstractServerResource {

	protected Session login(){
		RCentral rc =  getContext().getAttributeObject(RCentral.class.getCanonicalName(), RCentral.class) ;
		String wname = getInnerRequest().getAttribute("wname");
		Session session = rc.login(wname) ;
		return session ;
	}
}
