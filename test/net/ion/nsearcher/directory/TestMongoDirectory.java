package net.ion.nsearcher.directory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import net.ion.framework.db.Page;
import net.ion.framework.util.Debug;
import net.ion.framework.util.IOUtil;
import net.ion.framework.util.ListUtil;
import net.ion.framework.util.StringUtil;
import net.ion.nsearcher.ISTestCase;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.config.CentralConfig;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.indexer.storage.mongo.SimpleCentralConfig;
import net.ion.nsearcher.indexer.storage.mongo.StorageFac;
import net.ion.nsearcher.search.SearchResponse;
import net.ion.nsearcher.search.Searcher;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;

public class TestMongoDirectory extends ISTestCase {

	private CentralConfig config ;
	private Analyzer analyzer = new MyKoreanAnalyzer();
	public void setUp() throws Exception {
		super.setUp();

		config = SimpleCentralConfig.create(StorageFac.createToMongo("61.250.201.78", "search", "storageTest"));
	}
	
	public void testIndex() throws Exception {
		writeDocument(config, analyzer) ;
		
		long start = System.currentTimeMillis() ;
		Central c = config.build() ; 
		Searcher searcher = c.newSearcher() ;
		
		SearchResponse response = searcher.search("bleujin");
		Debug.line(response.size(), System.currentTimeMillis() - start, response.getDocument() ) ;
	}
	
	public void testFileIndex() throws Exception {
		Central c = writeDocument(analyzer) ;
		
		Searcher searcher = c.newSearcher() ;
		long start = System.currentTimeMillis() ;
		
		SearchResponse response = searcher.search("bleujin");
		Debug.line(response.size(), System.currentTimeMillis() - start, response.getDocument() ) ;
	}
	
	
	public void testApply() throws Exception {
		Central c = writeDocument(config, analyzer) ;
		
		Central newC = SimpleCentralConfig.createCentral(StorageFac.createToMongo("61.250.201.78", "search", "storageTest")) ;
		newC.newSearcher().search("bleujin").debugPrint() ;
		Debug.line('@', config.buildDir().getLockID(), newC.dir().getLockID()) ;

		Indexer writer = c.newIndexer() ;
		writer.index(analyzer, new IndexJob<Void>() {
			public Void handle(IndexSession session) throws Exception {
				session.insertDocument(MyDocument.testDocument().keyword("name", "bleujin")) ;
				return null;
			}
		}) ;

		newC.newSearcher().search("bleujin").debugPrint() ;
		Debug.line() ;
	}
	
	
	public void testSpeed() throws Exception {
		Directory[] dirs = new Directory[]{StorageFac.createToMongo("61.250.201.78", "search", "storageTest"), new MMapDirectory(new File("c:/temp/")  )} ;
		for (Directory dir : dirs) {
			long start = System.currentTimeMillis() ;
			indexSpeed(dir);
			Debug.line(dir.getClass(), System.currentTimeMillis() - start) ;
		}
	}

	private void indexSpeed(Directory dir) throws LockObtainFailedException, IOException {
		Central cram = SimpleCentralConfig.createCentral(dir) ;
		Indexer indexer = cram.newIndexer();
		
		final String readString = IOUtil.toString(new FileReader(new File("test/" + StringUtil.replace(this.getClass().getCanonicalName(), ".", "/")) + ".java")) ;
		final String extString = IOUtil.toString(new FileReader(new File("build.xml"))) ;
		indexer.index(new IndexJob<Void>() {

			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 1000; i++) {
					MyDocument doc = MyDocument.testDocument();
					doc.keyword("name", "bleujin") ;
					doc.text("file", extString) ;
					session.insertDocument(doc) ;
				}
				return null;
			}
		}) ;
	}
	
	
	public void xtestArrayEqual() throws Exception {
		String[] a1 = new String[]{"jin", "hero"};
		String[] a2 = new String[]{"hero", "jin"};
		String[] a3 = new String[]{"bleu", "jin"};
		String[] a4 = new String[]{"hero", "jin", "bleu"};
		
		
		// assertEquals(true, Arrays.equals(a1, a2)) ;
		
		HashSet<String> s1 = new HashSet<String>(ListUtil.toList(a1)) ;
		HashSet<String> s2 = new HashSet<String>(ListUtil.toList(a2)) ;
		HashSet<String> s3 = new HashSet<String>(ListUtil.toList(a3)) ;
		HashSet<String> s4 = new HashSet<String>(ListUtil.toList(a4)) ;
		
		assertEquals(true, s1.equals(s2)) ;
		assertEquals(false, s1.equals(s4)) ;
		assertEquals(false, s4.equals(s1)) ;
	}
	
	
}
