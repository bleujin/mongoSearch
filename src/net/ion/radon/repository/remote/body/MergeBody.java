package net.ion.radon.repository.remote.body;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import net.ion.radon.repository.Node;

public class MergeBody implements Serializable{

	private static final long serialVersionUID = -3467717274750714281L;

	private ArrayList<Node> nodes ;
	private MergeBody(ArrayList<Node> nodes) {
		this.nodes = nodes ;
	}

	public static MergeBody create(ArrayList<Node> nodes) {
		return new MergeBody(nodes);
	}

	public ArrayList<Node> getNodes() {
		return nodes;
	}

	
	
	
}
