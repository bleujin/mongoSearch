package net.ion.nsearcher.directory;

import java.io.File;
import java.io.FileInputStream;

import junit.framework.TestCase;
import net.ion.framework.db.DBController;
import net.ion.framework.db.IDBController;
import net.ion.framework.db.Page;
import net.ion.framework.db.manager.DBManager;
import net.ion.framework.db.manager.OracleCacheDBManager;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.nsearcher.Searcher;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.indexer.storage.mongo.SimpleCentralConfig;
import net.ion.nsearcher.indexer.storage.mongo.StorageFac;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.WorkspaceOption;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexFileNameFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class TestMongoBig extends TestCase {

	public void testConnectMongo() throws Exception {
		RepositoryCentral rc = RepositoryCentral.create("social.i-on.net", 20001);
		Session session = rc.login("icss_mongo", "iyaa.blocks");

		session.getWorkspace("iyaa.blocks", WorkspaceOption.NONE);
		Debug.line(session.createQuery().find().count());
		rc.unload();
	}

	public void testLocalCopy() throws Exception {
		Directory src = StorageFac.createToMongo("social.i-on.net:20001", "icss_mongo", "iyaa");
		Directory dest = FSDirectory.open(new File("d:/project/indo/"));
		IndexFileNameFilter filter = IndexFileNameFilter.getFilter();
		for (String file : src.listAll()) {
			if (filter.accept(null, file)) {
				src.copy(dest, file, file);
			}
		}
	}

	public void testConnect() throws Exception {
		Directory dir = StorageFac.createToMongo("social.i-on.net:20001", "icss_mongo", "iyaa");
		Central c = SimpleCentralConfig.createCentral(dir);

		Searcher searcher = c.newSearcher();
		searcher.searchTest("iyaa").debugPrint(Page.TEN);

		c.destroySelf();
	}

	public void testCreateIndex() throws Exception {
		Directory dir = StorageFac.createToMongo("61.250.201.78", "search", "bigTest");
		Central c = SimpleCentralConfig.createCentral(dir);

		Indexer di = c.newIndexer();
		final String buildString = IOUtil.toString(new FileInputStream(new File("build.xml")));
		di.index(new StandardAnalyzer(Version.LUCENE_CURRENT), new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				for (int i : ListUtil.rangeNum(100000)) {
					MyDocument doc = MyDocument.testDocument();
					doc.add(MyField.number("index", i));
					doc.add(MyField.keyword("rstring", RandomUtil.nextRandomString(10)));
					doc.add(MyField.text("text", buildString));
					session.insertDocument(doc);
					if (i % 1000 == 0)
						System.out.print('.');
				}
				return null;
			}
		}) ;
	}

	public void testSearch() throws Exception {
		Directory dir = StorageFac.createToMongo("61.250.201.78", "search", "bigTest");
		Central c = SimpleCentralConfig.createCentral(dir);

		Searcher searcher = c.newSearcher();
		Debug.line(searcher.searchTest("").getTotalCount());
		// searcher.searchTest("index:888").debugPrint(Page.ALL) ;

	}

	public void testDB() throws Exception {
		DBManager dbm = new OracleCacheDBManager("dev-oracle.i-on.net:1521/dev10g", "dev_ics5", "dev_ics5");
		IDBController dc = new DBController(dbm);
		dc.initSelf();

		Debug.line(dc.getRows("select count(*) from user_task_trace_tblc"));

		dc.destroySelf();
	}

}
