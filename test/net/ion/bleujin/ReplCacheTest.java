package net.ion.bleujin;

import java.util.Date;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

import junit.framework.TestCase;

public class ReplCacheTest extends TestCase {

	public void testStart() throws Exception {
		DefaultCacheManager cm = new DefaultCacheManager(
		      GlobalConfigurationBuilder.defaultClusteredBuilder()
		         .transport().addProperty("configurationFile", "resource/config/jgroups-tcp.xml")
		         .build(),
		      new ConfigurationBuilder()
		         .clustering().cacheMode(CacheMode.REPL_SYNC)
		         .build()
		   );
		
		cm.start() ;
		
		Cache<Object, Object> cache = cm.getCache("workspace");
		cache.addListener(new SampleListener());
		while(true){
			cache.put("date", "hero " + new Date()) ;
			Thread.sleep(800) ;
		}
	}
	
	public void testGetter() throws Exception {
		DefaultCacheManager cacheManager = new DefaultCacheManager(
			      GlobalConfigurationBuilder.defaultClusteredBuilder()
			         .transport().addProperty("configurationFile", "resource/config/jgroups-tcp.xml")
			         .build(),
			      new ConfigurationBuilder()
			         .clustering().cacheMode(CacheMode.REPL_SYNC)
			         .build()
			   );
		cacheManager.start();

		while (true) {
			Cache<Object, Object> cache = cacheManager.getCache("workspace");

			System.out.println(cache.get("date"));
			Thread.sleep(900);
		}

	}
}
