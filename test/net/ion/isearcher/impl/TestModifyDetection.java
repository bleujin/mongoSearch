package net.ion.isearcher.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.ObjectUtil;
import net.ion.framework.util.RandomUtil;
import net.ion.isearcher.common.MyDocument;
import net.ion.isearcher.common.MyField;
import net.ion.isearcher.indexer.storage.mongo.DistributedDirectory;
import net.ion.isearcher.indexer.storage.mongo.MongoDirectory;
import net.ion.isearcher.indexer.storage.mongo.RefreshStrategy;
import net.ion.isearcher.indexer.storage.mongo.StorageFac;
import net.ion.isearcher.indexer.write.IWriter;
import net.ion.isearcher.searcher.MyKoreanAnalyzer;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;

import com.mongodb.Mongo;

public class TestModifyDetection extends TestCase {

	public void testRunIndexer() throws Exception {
		Directory dir = getDir();
		Central cen = Central.createOrGet(dir);

		MyKoreanAnalyzer analyzer = new MyKoreanAnalyzer();

		int i = 0 ;
		while (i++ < 10) {
			IWriter iw = cen.newIndexer(analyzer);
			iw.begin("my..");
			iw.deleteAll();
			for (int k = 0; k < RandomUtil.nextInt(7) + 3; k++) {
				iw.insertDocument(MyDocument.testDocument().add(MyField.number("index", RandomUtil.nextInt(1000))).add(MyField.keyword("name", "bleujin")));
				Thread.sleep(5);
			}
			Debug.line("before" + i, currentGen(dir)) ;
			iw.commit();
			iw.end();
			Debug.line("after" + i, currentGen(dir)) ;
			
			Thread.sleep(100);
		}
	}
	
	private long currentGen(Directory dir) throws CorruptIndexException, IOException{
		SegmentInfos latest = new SegmentInfos();
		latest.read(dir);
		return latest.getGeneration();
	}

	private DistributedDirectory getDir() throws IOException {
		// Directory dir = FSDirectory.open(new File("c:/temp/indextest")) ;
		Mongo mongo = new Mongo("61.250.201.78");
		RefreshStrategy rs = RefreshStrategy.createSchedule(Executors.newSingleThreadScheduledExecutor(), 200, TimeUnit.MILLISECONDS) ;
		DistributedDirectory dir = new DistributedDirectory(new MongoDirectory(mongo, "test", "prefix"), rs);
		return dir;
	}

	public void testDetectStart() throws Exception {
		new Thread() {
			public void run() {
				try {
					new TestModifyDetection().testRunIndexer();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();

		Thread.sleep(500);
		int i = 0;
		// NRTCachingDirectory cdir = new NRTCachingDirectory(getDir(), 0.001, 60) ;
		Directory cdir = getDir() ;
		IndexReader reader = IndexReader.open(cdir);
		while (i++ < 30) {

			IndexReader newReader = ObjectUtil.coalesce(IndexReader.openIfChanged(reader), reader) ;
			IndexSearcher searcher = new IndexSearcher(newReader);
			long start = System.currentTimeMillis();
			TopDocs docs = searcher.search(new MatchAllDocsQuery(), 10);
			// for (Do : docs.scoreDocs) {
			//				
			// }
			
			Debug.debug(IndexReader.lastModified(cdir), docs.totalHits, System.currentTimeMillis() - start, reader, IndexReader.openIfChanged(reader));

			// Debug.line(docs.totalHits, System.currentTimeMillis() - start, IndexReader.openIfChanged(reader), dir.getLockID()) ;
			
			Thread.sleep(200);
		}
	}
	
	public void infinityIndex() throws Exception {
		Directory dir = getDir();
		Central cen = Central.createOrGet(dir);

		MyKoreanAnalyzer analyzer = new MyKoreanAnalyzer();

		int i = 0 ;
		while (i++ < 10000) {
			IWriter iw = cen.newIndexer(analyzer);
			iw.begin("my..");
			iw.deleteAll();
			for (int k = 0; k < RandomUtil.nextInt(7) + 3; k++) {
				iw.insertDocument(MyDocument.testDocument().add(MyField.number("index", RandomUtil.nextInt(1000))).add(MyField.keyword("name", "bleujin")));
				Thread.sleep(200);
			}
//			iw.commit();
			iw.rollback() ;
			iw.end();
			Debug.debug("after" + i, currentGen(dir)) ;
			Thread.sleep(2000);
		}
	}
	
	
	public void testInfo() throws Exception {
		
		for (int i = 0; i < 10; i++) {
			Mongo mongo = new Mongo("61.250.201.78");
			DistributedDirectory dir = new DistributedDirectory(new MongoDirectory(mongo, "test", "prefix"));
			Central c = Central.createOrGet(dir) ;
			IndexReader ir = c.newReader().getIndexReader().openIfChanged(c.newReader().getIndexReader()) ;
			
			if (ir == null) ir =  c.newReader().getIndexReader() ;
			
			Debug.line(ir.getIndexCommit().getUserData(),  IndexReader.lastModified(dir));
			Thread.sleep(1000) ;
		}

	}

	private void print(Directory dir) throws CorruptIndexException, IOException{
		SegmentInfos latest = new SegmentInfos();
		latest.read(dir);
		long currentGen = latest.getGeneration();
		
		String files[] = dir.listAll();
		Debug.line(currentGen, files) ;
		for (int i = 0; i < files.length; i++) {
			String fileName = files[i];
			if (!fileName.startsWith("segments") || fileName.equals("segments.gen")){
				continue ;
			}
			if (SegmentInfos.generationFromSegmentsFileName(fileName) >= currentGen)				
				continue;

			SegmentInfos sis = new SegmentInfos();
			try {
				sis.read(dir, fileName);
			} catch (FileNotFoundException fnfe) {
				sis = null;
			}
			Debug.line(sis) ;
		}
	}
	

	public void testSearcher() throws Exception {

		int i = 0;
		while (i++ < 10) {
			Directory dir = getDir();
			IndexReader reader = IndexReader.open(dir);

			Central cen = Central.createOrGet(dir);
			ISearcher searcher = cen.newSearcher();
			searcher.reopen();

			Debug.line('?', cen.getIndexReader());

			searcher.searchTest("bleujin").debugPrint(Page.ALL);
			Thread.sleep(300);
		}

	}

}
