package net.ion.radon.repository.exception;

public class SearchRuntimeException extends IllegalStateException{

	private SearchRuntimeException(Throwable cause) {
		super(cause) ;
	}

	public static SearchRuntimeException create(Throwable cause){
		return new SearchRuntimeException(cause) ;
	}
}
