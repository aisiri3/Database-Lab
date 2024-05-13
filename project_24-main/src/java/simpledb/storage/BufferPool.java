package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    public int numPages;
    private ConcurrentHashMap<PageId, Page> pageBuffer;

    // declare lru hashmap for lru
    private ConcurrentHashMap<PageId, Integer> lru;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageBuffer = new ConcurrentHashMap<>(numPages);
        this.lru = new ConcurrentHashMap<>(numPages);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        switch (perm) {
            case READ_ONLY:
                this.lockManager.acquireReadLock(tid, pid);
                break;
            case READ_WRITE:
                this.lockManager.acquireWriteLock(tid, pid);
                break;
            default:
                throw new DbException("Invalid permission requested.");
        }

        synchronized (this) {
            if (pageBuffer.containsKey(pid)) { // if page buffer contains page, return page
                this.lru.replace(pid, 0);
                for (PageId pageId : this.lru.keySet()) {
                    this.lru.replace(pageId, this.lru.get(pageId) + 1);
                }
                return pageBuffer.get(pid);
            } else { // if page buffer does not contain page
                Page page;

                if (pageBuffer.size() < numPages) { // if page buffer still has space, add page and return page, update
                                                    // lru
                    page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    pageBuffer.put(pid, page);
                    this.lru.put(pid, 0);
                    for (PageId pageId : this.lru.keySet()) {
                        this.lru.replace(pageId, this.lru.get(pageId) + 1);
                    }
                    return page;
                } else { // if page buffer no space and dont contain page, evict, add page, return page,
                         // update lru
                    this.evictPage();
                    page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    pageBuffer.put(pid, page);
                    this.lru.put(pid, 0);
                    for (PageId pageId : this.lru.keySet()) {
                        this.lru.replace(pageId, this.lru.get(pageId) + 1);
                    }
                    return page;
                }
            }
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.deleteLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2

        if (this.lockManager.getTransactionOfPages(tid) == null)
            return;

        // get all pages held by transaction id
        Set<PageId> pageIds = this.lockManager.getTransactionOfPages(tid);

        // check if committed or not
        if (commit) {
            // if committed, flush all pages
            for (PageId pageId : pageIds) {
                try {
                    this.flushPage(pageId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // else if not committed, abort and discard page from buffer pool
        else {
            for (PageId pageId : pageIds) {
                this.discardPage(pageId);
            }
        }

        // release all locks
        for (PageId pageId : pageIds) {
            this.lockManager.deleteLock(tid, pageId);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // get dbfile using table id
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        // insert tuple to file by calling file.insertTuple(), returns list of modified
        // pages
        ArrayList<Page> modifiedPages = (ArrayList<Page>) file.insertTuple(tid, t);
        // iterate through mofified pages and make them dirty
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            // if page buffer has no space
            if (this.pageBuffer.size() >= this.numPages) {
                // if page buffer has no space and does not contain page, evict, add and update
                // lru
                if (!this.pageBuffer.containsKey(page.getId())) {
                    this.evictPage();
                    this.pageBuffer.put(page.getId(), page);
                    this.lru.put(page.getId(), 0);
                } else { // if page buffer has no space but already has page inside
                    this.lru.replace(page.getId(), this.lru.get(page.getId()) + 1);
                }

            } else { // if page buffer still has space
                if (this.pageBuffer.containsKey(page.getId())) {
                    // update lru count
                    this.lru.replace(page.getId(), this.lru.get(page.getId()) + 1);
                } else { // page buffer has space but does not contain page
                    this.pageBuffer.put(page.getId(), page);
                    this.lru.put(page.getId(), 0);
                }
            }
        }
        // update all lru count
        for (PageId pageId : this.lru.keySet()) {
            this.lru.replace(pageId, this.lru.get(pageId) + 1);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // get table id using tuple
        int tableId = t.getRecordId().getPageId().getTableId();
        // get file from table id
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        // remove tuple from file, return list of modified pages
        ArrayList<Page> modifiedPages = (ArrayList<Page>) file.deleteTuple(tid, t);
        // iterate through all pages in modified pages and make them dirty
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);

            // if page buffer has no space
            if (this.pageBuffer.size() >= this.numPages) {
                // if page buffer has no space and does not contain page, evict, add and update
                // lru
                if (!this.pageBuffer.containsKey(page.getId())) {
                    this.evictPage();
                    this.pageBuffer.put(page.getId(), page);
                    this.lru.put(page.getId(), 0);
                } else { // if page buffer has no space but already has page inside
                    this.lru.replace(page.getId(), this.lru.get(page.getId()) + 1);
                }

            } else { // if page buffer still has space
                if (this.pageBuffer.containsKey(page.getId())) {
                    // update lru count
                    this.lru.replace(page.getId(), this.lru.get(page.getId()) + 1);
                } else { // page buffer has space but does not contain page
                    this.pageBuffer.put(page.getId(), page);
                    this.lru.put(page.getId(), 0);
                }
            }

        }
        // update all lru count
        for (PageId pageId : this.lru.keySet()) {
            this.lru.replace(pageId, this.lru.get(pageId) + 1);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

        for (Page page : this.pageBuffer.values()) {
            this.flushPage(page.getId());
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pageBuffer.remove(pid);
        this.lru.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        if (pageBuffer.containsKey(pid)) {
            Page pg = pageBuffer.get(pid);
            if (pg.isDirty() != null) {
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pg);
            }
        }
        ;
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int oldestPageCounter = 0;
        PageId oldestPageId = null;

        // iterate through LRU to check for least used page, implement NO STEAL here, do
        // not evict dirty pages
        for (PageId pageId : this.lru.keySet()) {
            if ((this.lru.get(pageId) > oldestPageCounter) && (this.pageBuffer.get(pageId).isDirty() == null)) {
                oldestPageCounter = this.lru.get(pageId);
                oldestPageId = pageId;
            }
        }

        // if oldestpageid is null, means no page to evict
        if (oldestPageId == null) {
            throw new DbException("no page to evict");
        }

        // pageid should now contain least used page
        // try to flush the page
        try {
            this.flushPage(oldestPageId);
            this.discardPage(oldestPageId);
        } catch (IOException e) {
            throw new DbException("error when trying to flush page");
        }
    }

}
