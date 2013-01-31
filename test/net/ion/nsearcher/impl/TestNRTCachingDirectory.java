package net.ion.nsearcher.impl;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import net.ion.framework.util.Debug;
import net.ion.framework.util.InfinityThread;
import net.ion.framework.util.ListUtil;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.indexer.storage.mongo.DistributedDirectory;
import net.ion.nsearcher.indexer.storage.mongo.MongoDirectory;
import net.ion.nsearcher.indexer.storage.mongo.RefreshStrategy;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.mongodb.Mongo;

public class TestNRTCachingDirectory extends TestCase {

	public void testDefault() throws Exception {

		final ExecutorService es = Executors.newFixedThreadPool(5);

		NRTCachingDirectory dir = makeDir();

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
		IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_36, analyzer);
		conf.setMergeScheduler(dir.getMergeScheduler());
		final IndexWriter writer = new IndexWriter(dir, conf);
		writer.commit() ;

		final IndexReader reader = IndexReader.open(dir);
		final IndexSearcher searcher = new IndexSearcher(reader);

		es.submit(new Callable<Boolean>() {
			public Boolean call() throws Exception {
				try {
					for (int i = 0; i < 10; i++) {
						MyDocument doc = MyDocument.testDocument().add(MyField.number("time", new Date().getTime())).add(MyField.keyword("name", "kkkk")).add(MyField.number("index", i));
						writer.addDocument(doc.toLuceneDoc());
					}
					writer.commit();
					Debug.line("commit") ;
				} finally {
					Thread.sleep(200) ;
					es.submit(this) ;
				}
				return true;
			}
		});
		
		es.submit(new Callable<Integer>() {

			private IndexReader myReader = null ;
			private IndexSearcher mySearcher = null ;
			private long index = 0L;
			
			public Integer call() throws Exception {
				TopDocs docs = null ;
				try {
					if (myReader == null) {
						this.myReader = reader ;
						this.mySearcher = searcher ;
					}
					if (this.myReader != null) {
						IndexReader newReader = IndexReader.openIfChanged(myReader) ;
						if (newReader != null ) {
							this.myReader = newReader ;
							this.mySearcher = new IndexSearcher(this.myReader) ;
						}
					}
					
					
					Query query = new QueryParser(Version.LUCENE_36, "name", new StandardAnalyzer(Version.LUCENE_36)).parse("name:kkkk") ;
					docs = mySearcher.search(query, 1000);
					
				} finally {
					Debug.line(docs.totalHits, index++, this.myReader) ;
					
					Thread.sleep(150) ;
					es.submit(this) ;
					return (docs== null) ? 0 : docs.totalHits;
				}
			}
		}) ;

		
		new InfinityThread().startNJoin() ;
	}

	private NRTCachingDirectory makeDir() throws IOException {
		// Directory fsDir = FSDirectory.open(new File("/path/to/index"));
		Directory fsDir = new RAMDirectory();
//		new IndexWriter(fsDir, new StandardAnalyzer(Version.LUCENE_36), true, MaxFieldLength.UNLIMITED).commit() ;
		
		NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(fsDir, 5.0, 60.0);
		return cachedFSDir;
	}
	
	
	public void testDetect() throws Exception {
		final ExecutorService es = Executors.newFixedThreadPool(5);

		NRTCachingDirectory dir = getDir();

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
		IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_36, analyzer);
		conf.setMergeScheduler(dir.getMergeScheduler());
		final IndexWriter writer = new IndexWriter(dir, conf);
		writer.commit() ;
		
		IndexReader reader = IndexReader.open(dir) ;

		es.submit(new Callable<Boolean>() {
			public Boolean call() throws Exception {
				try {
					for (int i = 0; i < 10; i++) {
						MyDocument doc = MyDocument.testDocument().add(MyField.number("time", new Date().getTime())).add(MyField.keyword("name", "kkkk")).add(MyField.number("index", i));
						writer.addDocument(doc.toLuceneDoc());
					}
					writer.commit();
					Debug.line("commit") ;
				} finally {
					Thread.sleep(500) ;
					es.submit(this) ;
				}
				return true;
			}
		}) ;
		new InfinityThread().startNJoin() ;
		
//		while(true){
//			Debug.line(IndexReader.openIfChanged(reader)) ;
//			Thread.sleep(500) ;
//		}
		
	}
	
	public void testOtherProcess() throws Exception {
		DistributedDirectory dir = new DistributedDirectory(new MongoDirectory(new Mongo("61.250.201.78"), "test", "prefix"));
		while(true){
			IndexReader reader = IndexReader.open(dir) ;
			dir.sync(ListUtil.toList(dir.listAll())) ;
			Debug.line(IndexReader.openIfChanged(reader), IndexReader.lastModified(dir), IndexReader.listCommits(dir)) ;
			Thread.sleep(500) ;
		}
	}
	

	private NRTCachingDirectory getDir() throws IOException {
		// Directory dir = FSDirectory.open(new File("c:/temp/indextest")) ;
		Mongo mongo = new Mongo("61.250.201.78");
		RefreshStrategy rs = RefreshStrategy.createSchedule(Executors.newSingleThreadScheduledExecutor(), 200, TimeUnit.MILLISECONDS) ;
		DistributedDirectory dir = new DistributedDirectory(new MongoDirectory(mongo, "test", "prefix"));
		return new NRTCachingDirectory(dir, 0.01, 60.0);
	}
}
