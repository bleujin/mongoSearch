package net.ion.radon.repository;

import junit.framework.TestCase;
import net.ion.framework.db.Page;
import net.ion.nsearcher.config.CentralConfig;
import net.ion.radon.core.PageBean;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import com.mongodb.Mongo;

public class TestSearchRepositoryCentral extends TestCase {

	public void testLogin() throws Exception {
		
		Mongo mongo = new Mongo("61.250.201.197");
		String dbName = "ICS_MONGO";
		String userId = "ics";
		String userPwd = "ics";

		SearchRepositoryCentral sc = new SearchRepositoryCentral(mongo, dbName, userId, userPwd, CentralConfig.newRam().build());
		
		SearchSession ss = sc.login(dbName, "test") ;
		ss.dropWorkspace() ;
		ss.newNode().put("name", "bleujin").getSession().commit() ;

		ss.createQuery().find().debugPrint(PageBean.ALL) ;
		
		ss.waitForFlushed() ;
		ss.createSearchQuery().find().debugPrint() ;
	}
}
