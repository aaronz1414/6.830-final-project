package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	
	Map<PageId, Map<TransactionId, Integer>> readLocks = new ConcurrentHashMap<>();
	Map<PageId, Map<TransactionId, Integer>> writeLocks = new ConcurrentHashMap<>();

	private static long TIMEOUT = 1000;
	
	/**
	 * 
	 * @param pid
	 * @param tid
	 * @return True if the lock was acquired, false if not
	 * @throws TransactionAbortedException 
	 * @throws IOException 
	 */
	public synchronized boolean acquireLock(PageId pid, TransactionId tid, boolean write) throws TransactionAbortedException {
		
		if (write && isUpgradeable(pid, tid)) {
			readLocks.get(pid).remove(tid);
		} else if (hasLockByTransaction(writeLocks, pid, tid) || (hasLockByTransaction(readLocks, pid, tid) && !write)) {
			return true;
		}
		
		long startTimeMillis = System.currentTimeMillis();
		while (hasLock(writeLocks, pid) || (hasLock(readLocks, pid) && write)) {
			if (!sleep(10) || System.currentTimeMillis() - startTimeMillis > TIMEOUT) {
				try {
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (IOException e) { }
				throw new TransactionAbortedException();
			}
		}
		
		if (write) {
			addTransaction(writeLocks, pid, tid);
		} else {
			addTransaction(readLocks, pid, tid);
		}
		
		return true;
	}
	
	private boolean sleep(long timeInMillis) {
		try {
			Thread.sleep(timeInMillis);
		} catch (InterruptedException e) {
			return false;
		}
		
		return true;
	}
	
	private boolean isUpgradeable(PageId pid, TransactionId tid) {
		return readLocks.containsKey(pid) &&
				readLocks.get(pid).containsKey(tid) &&
				readLocks.get(pid).size() == 1 &&
				!hasLock(writeLocks, pid);
	}
	
	private void addTransaction(Map<PageId, Map<TransactionId, Integer>> locks, PageId pid, TransactionId tid) {
		if (!locks.containsKey(pid)) {
			locks.put(pid, new ConcurrentHashMap<>());
		}
		
		locks.get(pid).put(tid, 0);
	}
	
	private boolean hasLockByTransaction(Map<PageId, Map<TransactionId, Integer>> locks, PageId pid, TransactionId tid) {
		return locks.containsKey(pid) && locks.get(pid).containsKey(tid);
	}
	
	private boolean hasLock(Map<PageId, Map<TransactionId, Integer>> locks, PageId pid) {
		return locks.containsKey(pid) && locks.get(pid).size() > 0;
	}

	public synchronized boolean hasLock(TransactionId tid, PageId pid) {
		return (readLocks.containsKey(pid) && readLocks.get(pid).containsKey(tid))
				|| (writeLocks.containsKey(pid) && writeLocks.get(pid).containsKey(tid));
	}
	
	public synchronized void release(PageId pid, TransactionId tid) {
		
		if (readLocks.containsKey(pid)) {
			readLocks.get(pid).remove(tid);
		}
		
		if (writeLocks.containsKey(pid)) {
			writeLocks.get(pid).remove(tid);
		}
	}
	
	public synchronized void release(List<PageId> pids, TransactionId tid) {
		for (PageId pid : pids) {
			release(pid, tid);
		}
	}
	
	public synchronized List<PageId> getPageIds(TransactionId tid, boolean write) {
		List<PageId> pageIds = new ArrayList<>();
		if (write) {
			pageIds.addAll(findLocks(writeLocks, tid));
		} else {
			pageIds.addAll(findLocks(readLocks, tid));
		}
		
		return pageIds;
	}
	
	private List<PageId> findLocks(Map<PageId, Map<TransactionId, Integer>> locks, TransactionId tid) {
		List<PageId> pageIds = new ArrayList<>();
		for (PageId pid : locks.keySet()) {
			if (locks.containsKey(pid) && locks.get(pid).containsKey(tid)) {
				pageIds.add(pid);
			}
		}
		
		return pageIds;
	}

}
