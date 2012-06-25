package net.ion.radon.repository;

import java.net.UnknownHostException;

import org.apache.commons.configuration.ConfigurationException;

import com.mongodb.MongoException;

import net.ion.framework.util.InstanceCreationException;
import net.ion.radon.core.Aradon;
import net.ion.radon.core.IService;
import net.ion.radon.core.context.OnEventObject;
import net.ion.radon.repository.remote.RemoteClient;

public class PlugInRunner implements OnEventObject{

	
	private String host ;
	private int port ;
	private String dbName ;
	
	private RepositoryCentral rc ;
	public PlugInRunner(){
		this("localhost", 27017, "test") ;
	}
	
	public PlugInRunner(String host, int port, String dbName){
		this.host = host ;
		this.port = port ;
		this.dbName = dbName ;
	}
	
	public void onEvent(AradonEvent event, IService service) {
		if (event.equals(AradonEvent.START)) {
			startPlugin(service.getAradon()) ;
		} else if (event.equals(AradonEvent.STOP)) {
			stopPlugin() ;
		}
	}

	private void stopPlugin() {
		rc.shutDown() ;
	}

	private void startPlugin(Aradon aradon) {
		try {
			this.rc = RepositoryCentral.create(host, port, dbName) ;
			SearchRepositoryCentral.testCreate() ;
			RemoteClient.attachSection(aradon, rc) ;
			
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e) ;
		} catch (MongoException e) {
			throw new IllegalArgumentException(e) ;
		} catch (ConfigurationException e) {
			throw new IllegalArgumentException(e) ;
		} catch (InstanceCreationException e) {
			throw new IllegalArgumentException(e) ;
		} catch (Exception e) {
			throw new IllegalArgumentException(e) ;
		}
	}

}
