package net.ion.radon.repository.search;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.InfinityThread;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.impl.Central;
import net.ion.isearcher.impl.ISearcher;
import net.ion.isearcher.indexer.storage.mongo.DistributedDirectory;
import net.ion.isearcher.indexer.storage.mongo.MongoDirectory;
import net.ion.isearcher.indexer.storage.mongo.RefreshStrategy;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;
import net.ion.isearcher.searcher.SearchRequest;
import net.ion.radon.repository.SearchRepositoryCentral;
import net.ion.radon.repository.SearchSession;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.mongodb.Mongo;

public class TestBigIndex extends TestCase {

	private SearchSession session;
	private Central mycen;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Mongo mongo = new Mongo("61.250.201.78");
		RefreshStrategy refresher = RefreshStrategy.createSchedule(Executors.newSingleThreadScheduledExecutor(), 5, TimeUnit.SECONDS);
		Directory dir = new DistributedDirectory(new MongoDirectory(mongo, "test", "bidxifle"));

		SearchRepositoryCentral rc = new SearchRepositoryCentral(mongo, "test", null, null, dir);
		session = rc.login("search", "bigindex");

		mycen = Central.createOrGet(dir);
	}

	public void xtestIndex() throws Exception {
		// session.dropWorkspace() ;
		String buildString = IOUtil.toString(new File("resource/aradon/build_plugin.xml").toURI());
		int loopMax = 300;
		for (int loop : ListUtil.rangeNum(loopMax)) {
			for (int i : ListUtil.rangeNum(1000)) {
				session.newNode().put("job", "bindex").put("index", i + 1000 * loop).put("rand", RandomUtil.nextRandomString(20)).put("buildString", buildString);
			}
			session.commit();
			session.waitForFlushed();
		}

	}

	public void xtestSearch() throws Exception {
		// session.createQuery().find().debugPrint(PageBean.ALL) ;

		// Debug.line(session.createSearchQuery().term("index", "333").find().getTotalCount()) ;
		ISearcher searcher = mycen.newSearcher();
		Debug.line(searcher.searchTest("index:333").getTotalCount());
	}

	public void testSearch() throws Exception {
		ISearcher searcher = mycen.newSearcher();
		for (int i = 0; i < 10; i++) {
			searcher.searchTest("index:" + RandomUtil.nextInt(1000)) ;
			Debug.line(System.currentTimeMillis()) ;
		}
		
	}
	
	public void testConcurrent() throws Exception {
		final ExecutorService es = Executors.newFixedThreadPool(5);

		IWriter iw = mycen.newDaemonIndexer(new MyKoreanAnalyzer());
		iw.begin("dddd");
		QueryParser parser = new QueryParser(Version.LUCENE_36, "IS-all", new MyKoreanAnalyzer());
		iw.deleteQuery(parser.parse("name:bleujin")) ;
		iw.end();

		
		es.submit(new Callable<Boolean>() {
			public Boolean call() throws Exception {
				try {
					Debug.line("Searched Completed");
					ISearcher searcher = mycen.newSearcher();
					searcher.searchTest("index:" + RandomUtil.nextInt(1000)).getTotalCount();
					Thread.sleep(50);
				} catch (Throwable th) {
					th.printStackTrace() ;
				} finally {
					es.submit(this);
				}
				return Boolean.TRUE;
			}
		});

		es.submit(new Callable<Boolean>() {
			public Boolean call() throws Exception {
				IWriter iw = mycen.newDaemonIndexer(new MyKoreanAnalyzer());
				iw.begin("myy");
				iw.insertDocument(MyDocument.testDocument().add(MyField.keyword("name", "bleujin")));
				iw.end();
				Debug.line("Indexed Daemon");
				Thread.sleep(400);
				es.submit(this);
				return Boolean.TRUE;
			}

		});

		new InfinityThread().startNJoin();
	}
	
	
	

}
