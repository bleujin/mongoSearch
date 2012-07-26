package net.ion.isearcher.directory;

import java.io.File;
import java.io.FileInputStream;

import javax.swing.plaf.ListUI;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexFileNameFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import net.ion.framework.db.DBController;
import net.ion.framework.db.IDBController;
import net.ion.framework.db.Page;
import net.ion.framework.db.manager.DBManager;
import net.ion.framework.db.manager.OracleCacheDBManager;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.DaemonIndexer;
import net.ion.isearcher.impl.ISearcher;
import net.ion.isearcher.indexer.storage.mongo.StorageFac;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.radon.core.PageBean;
import net.ion.radon.repository.RepositoryCentral;
import net.ion.radon.repository.Session;
import net.ion.radon.repository.WorkspaceOption;
import junit.framework.TestCase;

public class TestMongoBig extends TestCase {

	public void testConnectMongo() throws Exception {
		RepositoryCentral rc = RepositoryCentral.create("social.i-on.net", 20001);
		Session session = rc.login("icss_mongo", "iyaa.blocks");

		session.getWorkspace("iyaa.blocks", WorkspaceOption.NONE);

		Debug.line(session.createQuery().find().count());

		rc.unload();
	}

	public void testOptimize() throws Exception {
		Directory dir = StorageFac.createToMongo("social.i-on.net:20001", "icss_mongo", "iyaa");
		Central c = Central.createOrGet(dir);
		IWriter iw = c.newIndexer(null);
		iw.begin("my") ;
		iw.optimize() ;
		iw.end() ;
		c.destroySelf() ;
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

		Central c = Central.createOrGet(dir);

		ISearcher searcher = c.newSearcher();
		searcher.searchTest("iyaa").debugPrint(Page.TEN);

		c.destroySelf();
	}

	public void testCreateIndex() throws Exception {
		Directory dir = StorageFac.createToMongo("61.250.201.78", "search", "bigTest");
		Central c = Central.createOrGet(dir);

		IWriter di = c.newDaemonIndexer(new StandardAnalyzer(Version.LUCENE_CURRENT));

		String buildString = IOUtil.toString(new FileInputStream(new File("build.xml")));
		di.begin("test");
		for (int i : ListUtil.rangeNum(100000)) {
			MyDocument doc = MyDocument.testDocument();
			doc.add(MyField.number("index", i));
			doc.add(MyField.keyword("rstring", RandomUtil.nextRandomString(10)));
			doc.add(MyField.text("text", buildString));
			di.insertDocument(doc);
			if (i % 1000 == 0)
				System.out.print('.');
		}
		di.end();
	}

	public void testSearch() throws Exception {
		Directory dir = StorageFac.createToMongo("61.250.201.78", "search", "bigTest");
		Central c = Central.createOrGet(dir);

		ISearcher searcher = c.newSearcher();
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
