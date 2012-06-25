package net.ion.radon.repository.remote.rest;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import javax.swing.plaf.ListUI;

import net.ion.framework.parse.gson.JsonParser;
import net.ion.framework.rest.HTMLFormater;
import net.ion.framework.rest.IRequest;
import net.ion.framework.rest.IResponse;
import net.ion.framework.rest.XMLFormater;
import net.ion.framework.util.ListUtil;
import net.ion.radon.core.EnumClass.IFormat;
import net.ion.radon.core.representation.JsonObjectRepresentation;
import net.ion.radon.repository.INode;
import net.ion.radon.repository.Node;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.restlet.data.MediaType;
import org.restlet.representation.ObjectRepresentation;
import org.restlet.representation.OutputRepresentation;
import org.restlet.representation.Representation;

public class ListNodeRepresentation {

	
	private static XMLFormater XMLFormater = new XMLFormater() ;
	private static HTMLFormater HTMLFormater = new HTMLFormater() ;
	
	public static Representation toRepresentation(final List<? extends INode> nodes, IFormat format) {
		if (format == IFormat.JSON) {
			return new JsonObjectRepresentation(JsonParser.fromList(nodes)) ;
		} else if (format == IFormat.XML){
			List<Map<String, ? extends Object>> listMap = ListUtil.newList() ; 
			for (INode node : nodes) {
				listMap.add(node.toPropertyMap()) ;
			}
			return XMLFormater.toRepresentation(IRequest.EMPTY_REQUEST, listMap, IResponse.EMPTY_RESPONSE) ;
		} else if (format == IFormat.HTML) {
			List<Map<String, ? extends Object>> listMap = ListUtil.newList() ; 
			for (INode node : nodes) {
				listMap.add(node.toPropertyMap()) ;
			}
			return HTMLFormater.toRepresentation(IRequest.EMPTY_REQUEST, listMap, IResponse.EMPTY_RESPONSE) ;
		} else {
			return new OutputRepresentation(MediaType.APPLICATION_JAVA_OBJECT){

				@Override
				public void write(OutputStream outputstream) throws IOException {
					ByteArrayOutputStream bout = new ByteArrayOutputStream() ;
					new ObjectOutputStream(bout).writeObject(nodes) ;
					outputstream.write(bout.toByteArray()) ;
				}
				
			} ;
		}
		
	}
}
