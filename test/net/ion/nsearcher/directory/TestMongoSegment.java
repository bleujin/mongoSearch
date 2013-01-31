package net.ion.nsearcher.directory;

import junit.framework.TestCase;
import net.ion.framework.db.Page;
import net.ion.nsearcher.Searcher;
import net.ion.nsearcher.common.MyDocument;
import net.ion.nsearcher.common.MyField;
import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.index.IndexJob;
import net.ion.nsearcher.index.IndexSession;
import net.ion.nsearcher.index.Indexer;
import net.ion.nsearcher.indexer.storage.mongo.SimpleCentralConfig;
import net.ion.nsearcher.indexer.storage.mongo.StorageFac;
import net.ion.nsearcher.search.SearchRequest;
import net.ion.nsearcher.search.analyzer.MyKoreanAnalyzer;

import org.apache.lucene.analysis.kr.KoreanAnalyzer;
import org.apache.lucene.store.Directory;

public class TestMongoSegment extends TestCase {

	public void testCase1() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "seg");
		Central c = SimpleCentralConfig.createCentral(dir);
		
		Indexer writer = c.newIndexer() ;
		writer.index(new KoreanAnalyzer(), new IndexJob<Void>(){

			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 30 ; i++) {
					MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin").number("index", i));
					session.insertDocument(doc) ;
				}
				return null;
			}
		}) ;
	}

	public void testCase3() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "seg");
		Central c = SimpleCentralConfig.createCentral(dir) ;

		Indexer writer = c.newIndexer() ;
		writer.index(new KoreanAnalyzer(), new IndexJob<Void>() {

			public Void handle(IndexSession session) throws Exception {
				for (int i = 0; i < 30 ; i++) {
					MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin")).add(MyField.number("index", i));
					session.insertDocument(doc) ;
				}
				return null;
			}
		}) ;
	}
	
	public void testCase2() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "seg");
		Central c = SimpleCentralConfig.createCentral(dir) ;
		
		Indexer writer = c.newIndexer() ;
		KoreanAnalyzer analyzer = new KoreanAnalyzer();
		for (int i = 0; i < 30 ; i++) {
			final int idx = i ;
			writer.asyncIndex("", analyzer, new IndexJob<Void>() {
				public Void handle(IndexSession session) throws Exception {
					MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin")).add(MyField.number("index", idx));
					session.insertDocument(doc) ;
					return null;
				}
			}) ;
		}
		
	}
	
	
	public void xtestAppend() throws Exception {
		Directory dir = StorageFac.createToMongo("61.250.201.78", "search", "seg");
		Central c = SimpleCentralConfig.createCentral(dir);
		Indexer writer = c.newIndexer() ;
		KoreanAnalyzer analyzer = new KoreanAnalyzer();
		for (int i = 0; i < 30 ; i++) {
			final int idx = i ;
			writer.asyncIndex("dd", analyzer, new IndexJob<Void>() {
				public Void handle(IndexSession session) throws Exception {
					MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin")).add(MyField.number("index", idx));
					session.insertDocument(doc) ;
					return null;
				}
			}) ;
		}
	}
	
	public void xtestSearch() throws Exception {
		Directory dir = StorageFac.createToMongo("61.250.201.78", "search", "seg");
		Central c = SimpleCentralConfig.createCentral(dir) ;
		Searcher search = c.newSearcher() ;
		SearchRequest req = SearchRequest.create("", null, new MyKoreanAnalyzer()) ;
		req.page(Page.ALL) ;
		search.search(req).debugPrint(Page.ALL) ;
	}
	
	
	public void testCase4() throws Exception {
		Directory dir = StorageFac.testRecreateToMongo("61.250.201.78", "search", "seg");
		Central c = SimpleCentralConfig.createCentral(dir);
		
		Indexer writer = c.newIndexer() ;
		KoreanAnalyzer analyzer = new KoreanAnalyzer();
		for (int i = 0; i < 30 ; i++) {
			final int idx = i ;
			writer.index(analyzer, new IndexJob<Void>(){
				public Void handle(IndexSession session) throws Exception {
					MyDocument doc = MyDocument.testDocument().add(MyField.keyword("name", "bleujin")).add(MyField.number("index", idx));
					session.insertDocument(doc) ;
					return null;
				}
			}) ;
		}
	}
	

}
