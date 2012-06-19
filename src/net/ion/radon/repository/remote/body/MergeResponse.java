package net.ion.radon.repository.remote.body;

import java.io.Serializable;

public class MergeResponse implements Serializable{

	private static final long serialVersionUID = -3520613204324588755L;
	private int size ;
	private MergeResponse(int size) {
		this.size = size ;
	}

	public static MergeResponse create(int size) {
		return new MergeResponse(size);
	}

	public int size(){
		return size ;
	}
}
