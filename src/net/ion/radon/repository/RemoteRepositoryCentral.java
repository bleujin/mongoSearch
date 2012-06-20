package net.ion.radon.repository;

import net.ion.radon.aclient.NewClient;
import net.ion.radon.repository.remote.RemoteClient;

public class RemoteRepositoryCentral {

	
	private final NewClient ac ;
	private final String prePath  ;
	public RemoteRepositoryCentral(NewClient ac, String prePath) {
		this.ac = ac ;
		this.prePath = prePath ;
	}

	public static RemoteRepositoryCentral create(String host) {
		NewClient nc = NewClient.create();
		return new RemoteRepositoryCentral(nc, host + "/" + RemoteClient.DefaultSectionName);
	}
	public static RemoteRepositoryCentral create(NewClient ac, String host, String sectionName) {
		return new RemoteRepositoryCentral(ac, host + "/" + sectionName) ;
	}

	public RemoteSession login(String wname) {
		RemoteRepository repository = RemoteRepository.create(RemoteClient.create(ac, prePath)) ;
		return RemoteSession.create(repository, wname) ;
	}

	public void close() {
		ac.close() ;
	}

}
