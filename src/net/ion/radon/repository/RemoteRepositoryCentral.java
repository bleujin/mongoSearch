package net.ion.radon.repository;

import net.ion.radon.client.AradonClient;
import net.ion.radon.repository.remote.RemoteClient;

public class RemoteRepositoryCentral {

	
	private final AradonClient ac ;
	private final String sectionName  ;
	public RemoteRepositoryCentral(AradonClient ac, String sectionName) {
		this.ac = ac ;
		this.sectionName = sectionName ;
	}

	public static RemoteRepositoryCentral create(AradonClient ac) {
		return new RemoteRepositoryCentral(ac, RemoteClient.DefaultSectionName);
	}
	public static RemoteRepositoryCentral create(AradonClient ac, String sectionName) {
		return new RemoteRepositoryCentral(ac, sectionName) ;
	}

	public RemoteSession login(String wname) {
		RemoteRepository repository = RemoteRepository.create(RemoteClient.create(ac, sectionName)) ;
		return RemoteSession.create(repository, wname) ;
	}

}
