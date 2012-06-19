package net.ion.radon.repository.remote.body;

import java.io.Serializable;
import java.util.Collection;

import net.ion.radon.repository.Node;

public class MergeBody implements Serializable{

	private static final long serialVersionUID = -3467717274750714281L;

	private Collection<Node> nodes ;
	private MergeBody(Collection<Node> nodes) {
		this.nodes = nodes ;
	}

	public static MergeBody create(Collection<Node> nodes) {
		return new MergeBody(nodes);
	}

	public Collection<Node> getNodes() {
		return nodes;
	}

	
	
	
}
