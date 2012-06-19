package net.ion.isearcher.indexer.storage.mongo;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.ion.framework.util.Debug;

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
		return new ScheduleChecker(ses, delay, unit);
	}
}

class ReadOnlyStrategy extends RefreshStrategy {

	@Override
	public void startDetect() throws IOException {
		; // no action
	}

}

class ScheduleChecker extends RefreshStrategy {

	private final ScheduledExecutorService ses;
	private final int delay;
	private final TimeUnit unit;

	public ScheduleChecker(ScheduledExecutorService ses, int delay, TimeUnit unit) {
		this.ses = ses;
		this.delay = delay;
		this.unit = unit;
	}

	public void refresh() throws IOException {
		try {
			NosqlDirectory newDir = getNosqlDirectory().ifModified();
			if (getNosqlDirectory() != newDir){
				Debug.line("Detected Modified") ;
				setDir(getDistributedDirectory(), newDir) ;
				getDistributedDirectory().changeDir(newDir);
			}
		} finally {
			startDetect();
		}
	}

	@Override
	public void startDetect() throws IOException {
		final ScheduleChecker checker = this;
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
