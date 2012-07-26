package net.ion.isearcher.indexer.storage.mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.IndexReader;

import net.ion.framework.util.Debug;
import net.ion.framework.util.ListUtil;

public abstract class RefreshStrategy {

	public static RefreshStrategy READ_ONLY = new ReadOnlyStrategy();

	public abstract void startDetect() throws IOException;

	private DistributedDirectory dd;
	private NosqlDirectory ndir;

	protected void setDir(DistributedDirectory dd, NosqlDirectory ndir) {
		this.dd = dd;
		this.ndir = ndir;
	}

	protected DistributedDirectory getDistributedDirectory() {
		return dd;
	}

	protected NosqlDirectory getNosqlDirectory() {
		return ndir;
	}
	

	public final static RefreshStrategy createSchedule(ScheduledExecutorService ses, int delay, TimeUnit unit) {
		return new LastModifyChecker(ses, delay, unit);
	}
}

class ReadOnlyStrategy extends RefreshStrategy {

	@Override
	public void startDetect() throws IOException {
		; // no action
	}

}

class LastModifyChecker extends RefreshStrategy {

	private final ScheduledExecutorService ses;
	private final int delay;
	private final TimeUnit unit;
	private long lastModified = 0L ;
	public LastModifyChecker(ScheduledExecutorService ses, int delay, TimeUnit unit) {
		this.ses = ses;
		this.delay = delay;
		this.unit = unit;
	}

	public synchronized void refresh() throws IOException {
//		try {
//			
//			long lastModifiedOfDir = IndexReader.lastModified(getDistributedDirectory()) ;
//			if (this.lastModified == 0L) this.lastModified = lastModifiedOfDir ;
//			if (this.lastModified > 0L && this.lastModified < lastModifiedOfDir){
//				Debug.line("Detected Modified", this.lastModified, lastModifiedOfDir) ;
//				this.lastModified = lastModifiedOfDir ;
//				getDistributedDirectory().sync(ListUtil.syncList(getDistributedDirectory().listAll())) ;
//				NosqlDirectory newDir = getNosqlDirectory().newDir() ;
//				getDistributedDirectory().changeDir(newDir);
//				
//				Debug.line(getDistributedDirectory().listAll()) ;
//				for (String fileName : getDistributedDirectory().listAll()) {
//					
//				}
//			}
//		} finally {
//			startDetect();
//		}
	}

	@Override
	public void startDetect() throws IOException {
		final LastModifyChecker checker = this;
		ses.schedule(new Runnable() {
			public void run() {
				try {
					checker.refresh();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}, delay, unit);
	}
}


class DirNameChecker extends RefreshStrategy {

	private final ScheduledExecutorService ses;
	private final int delay;
	private final TimeUnit unit;

	public DirNameChecker(ScheduledExecutorService ses, int delay, TimeUnit unit) {
		this.ses = ses;
		this.delay = delay;
		this.unit = unit;
	}

	public synchronized void refresh() throws IOException {
		try {
			NosqlDirectory newDir = getNosqlDirectory().ifModified();
			if (getNosqlDirectory() != newDir){
//				Debug.line("Detected Modified") ;
				
				setDir(getDistributedDirectory(), newDir) ;
				getDistributedDirectory().changeDir(newDir);
			}
		} finally {
			startDetect();
		}
	}

	@Override
	public void startDetect() throws IOException {
		final DirNameChecker checker = this;
		ses.schedule(new Runnable() {
			public void run() {
				try {
					checker.refresh();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}, delay, unit);
	}
}
