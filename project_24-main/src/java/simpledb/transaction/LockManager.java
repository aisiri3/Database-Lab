package simpledb.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;
import simpledb.storage.PageId;

enum LockType {
    SHARED,
    EXCLUSIVE
}

//// Lock ////
class Lock {
    private TransactionId tid;
    private PageId pid;
    private LockType lockType;
    Set<TransactionId> holders; // owners -> all reads?
    private Map<TransactionId, LockType> acquirers; // acquire -> all writes?
    private int readCount = 0;
    private int writeCount = 0;

    public Lock(TransactionId tid, PageId pid, LockType lockType) {
        // this.tid = tid;
        // this.pid = pid;
        this.lockType = lockType;
        this.holders = new HashSet<TransactionId>();
        this.acquirers = new HashMap<TransactionId, LockType>();
    }

    // Read Lock
    public void readLock(TransactionId tid) {
        // guard clause
        if (this.holders.contains(tid) && this.lockType == LockType.SHARED)
            return;

        this.acquirers.put(tid, LockType.SHARED);
        synchronized (this) {
            try {
                while (this.writeCount != 0)
                    this.wait();

                this.readCount += 1;
                this.holders.add(tid);

                this.lockType = LockType.SHARED;

            } catch (Exception err) {
                err.printStackTrace();
            }
        }
        this.acquirers.remove(tid);
    }

    // write lock
    public void writeLock(TransactionId tid) {
        // guard clause
        if (this.holders.contains(tid) && this.lockType == LockType.EXCLUSIVE)
            return;
        if (this.acquirers.containsKey(tid))
            return;

        this.acquirers.put(tid, LockType.EXCLUSIVE);
        synchronized (this) {
            try {
                if (this.holders.contains(tid)) {
                    while (this.holders.size() > 1)
                        this.wait();

                    readUnlock(tid);
                }
                while (this.readCount > 0 || this.writeCount > 0)
                    this.wait();

                this.holders.add(tid);
                this.writeCount += 1;

                this.lockType = LockType.EXCLUSIVE;
            } catch (Exception err) {
                err.printStackTrace();
            }
        }

        this.acquirers.remove(tid);
    }

    public void readUnlock(TransactionId tid) {
        // Return if the value is not in the dict
        if (!holders.contains(tid))
            return;

        synchronized (this) {
            this.holders.remove(tid);
            this.readCount = this.readCount == 0 ? 0 : this.readCount - 1;

            notifyAll();
        }
    }

    public void writeUnlock(TransactionId tid) {
        // Return if the value is not in the dict
        if (!this.holders.contains(tid))
            return;

        synchronized (this) {
            this.holders.remove(tid);
            this.writeCount = this.writeCount == 0 ? 0 : this.writeCount - 1;
        }
        // notifyAll();
    }

    public void lock(TransactionId tid) {
        switch (lockType) {
            case SHARED:
                readLock(tid);
                break;
            case EXCLUSIVE:
                writeLock(tid);
                break;
        }
    }

    public void unlock(TransactionId tid) {
        switch (lockType) {
            case SHARED:
                readUnlock(tid);
                break;
            case EXCLUSIVE:
                writeUnlock(tid);
                break;
        }
    }

    public LockType getLockType() {
        return this.lockType;
    }

    public boolean heldBy(TransactionId tid) {
        return this.holders.contains(tid);
    }

    public Set<TransactionId> holders() {
        return holders;
    }

    public boolean isExclusive() {
        return this.lockType == LockType.EXCLUSIVE;
    }

}

//// Lock Manager ////
public class LockManager {
    private HashMap<PageId, Lock> locksInPage;
    private Map<TransactionId, Set<PageId>> pagesInTransaction;
    private Map<TransactionId, Set<TransactionId>> dependencyGraph;

    public LockManager() {
        locksInPage = new HashMap<PageId, Lock>();
        pagesInTransaction = new HashMap<TransactionId, Set<PageId>>();
        dependencyGraph = new HashMap<TransactionId, Set<TransactionId>>();
    }

    public synchronized void acquireReadLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Lock lock;

        synchronized (this) {
            if (getLock(tid, pid) == null)
                createLock(tid, pid);
            lock = getLock(tid, pid);
            if (lock.heldBy(tid))
                return;
            if (!lock.holders().isEmpty() && lock.isExclusive()) {
                dependencyGraph.put(tid, lock.holders());
                if (checkDeadlock(tid)) {
                    dependencyGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        lock.readLock(tid);
        synchronized (this) {
            dependencyGraph.remove(tid);
            if (getTransactionOfPages(tid) == null)
                createTransactionOfPages(tid);
            getTransactionOfPages(tid).add(pid);
        }
    }

    public void acquireWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Lock lock;
        synchronized (this) {
            if (getLock(tid, pid) == null)
                createLock(tid, pid);
            lock = getLock(tid, pid);
            if (lock.isExclusive() && lock.heldBy(tid))
                return;
            if (!lock.holders().isEmpty()) {
                dependencyGraph.put(tid, lock.holders());
                if (checkDeadlock(tid)) {
                    dependencyGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }

        lock.writeLock(tid);
        synchronized (this) {
            dependencyGraph.remove(tid);
            if (getTransactionOfPages(tid) == null)
                createTransactionOfPages(tid);
            getTransactionOfPages(tid).add(pid);
        }

    }

    private boolean checkDeadlock(TransactionId tid) {
        Set<TransactionId> visited = new HashSet<TransactionId>();
        Queue<TransactionId> queue = new LinkedList<TransactionId>();
        visited.add(tid);
        queue.offer(tid);
        while (!queue.isEmpty()) {
            TransactionId head = queue.poll();
            if (dependencyGraph.containsKey(head)) {
                for (TransactionId adj : dependencyGraph.get(head)) {
                    if (adj.equals(head))
                        continue;
                    if (visited.contains(adj))
                        return true;
                    visited.add(adj);
                    queue.offer(adj);
                }
            }
        }

        return false;
    }

    // locks
    public Lock getLock(TransactionId tid, PageId pid) {
        if (!locksInPage.containsKey(pid))
            return null;
        return locksInPage.get(pid);
    }

    public void createLock(TransactionId tid, PageId pid) {
        if (locksInPage.containsKey(pid))
            return;
        locksInPage.put(pid, new Lock(tid, pid, LockType.SHARED));
    }

    public void deleteLock(TransactionId tid, PageId pid) {
        synchronized (this) {
            if (!locksInPage.containsKey(pid))
                return;
            Lock lock = locksInPage.get(pid);
            lock.unlock(tid);
            pagesInTransaction.get(tid).remove(pid);
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return pagesInTransaction.containsKey(tid) && pagesInTransaction.get(tid).contains(pid);
    }

    // pages
    public Set<PageId> getTransactionOfPages(TransactionId tid) {
        if (!pagesInTransaction.containsKey(tid))
            return null;
        return pagesInTransaction.get(tid);
    }

    public void createTransactionOfPages(TransactionId tid) {
        if (pagesInTransaction.containsKey(tid))
            return;
        ConcurrentHashMap<PageId, Integer> map = new ConcurrentHashMap<>();
        Set<PageId> set = map.newKeySet();
        pagesInTransaction.put(tid, set);
    }

}