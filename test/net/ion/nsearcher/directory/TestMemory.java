package net.ion.nsearcher.directory;

import junit.framework.TestCase;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.indexer.storage.mongo.DistributedDirectory;
import net.ion.nsearcher.indexer.storage.mongo.MongoDirectory;
import net.ion.nsearcher.indexer.storage.mongo.SimpleCentralConfig;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;

import com.mongodb.Mongo;

public class TestMemory extends TestCase {

	public void xtestMongoConnect() throws Exception {
		RepositoryCentral rc = RepositoryCentral.create("61.250.201.78", 27017) ;
		Session session = rc.login("myspace") ;
		
		session.newNode().put("name", "bleujin").getSession().commit() ;
		session.createQuery().find().debugPrint(PageBean.ALL) ;
	}
	
	public void xtestSearchDir() throws Exception {
		Mongo mongo = new Mongo("61.250.201.78") ;
		MongoDirectory mdir = new MongoDirectory(mongo, "ICSS_MONGO", "search") ;
		DistributedDirectory dir = new DistributedDirectory(mdir);
		
		Central c = SimpleCentralConfig.create(dir).build() ;
	}
}
