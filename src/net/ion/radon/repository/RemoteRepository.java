package net.ion.radon.repository;

import java.util.Set;

import net.ion.radon.repository.remote.RemoteClient;

public class RemoteRepository implements Repository{

	
	private RemoteClient client ;
	private RemoteRepository(RemoteClient client){
		this.client = client ;
	}

	public static RemoteRepository create(RemoteClient client) {
		return new RemoteRepository(client);
	}

	public RemoteWorkspace getWorkspace(String wname, WorkspaceOption option) {
		return RemoteWorkspace.load(client, wname, option);
	}

	public Set<String> getWorkspaceNames() {
		return null;
	}


}
