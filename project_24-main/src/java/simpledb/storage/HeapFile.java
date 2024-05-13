package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */

    private File f;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tableID = pid.getTableId();
        int pageNumber = pid.getPageNumber();
        Database.getBufferPool();
        long offset = pageNumber * BufferPool.getPageSize();
        byte[] newPage = HeapPage.createEmptyPageData();

        try {
            FileInputStream fileInputStream = new FileInputStream(this.f);
            fileInputStream.skip(offset);
            fileInputStream.read(newPage);
            HeapPageId id = new HeapPageId(tableID, pageNumber);
            fileInputStream.close();
            return new HeapPage(id, newPage);

        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
        PageId pid = page.getId();
        int offset = BufferPool.getPageSize() * pid.getPageNumber();
        raf.seek(offset);
        raf.write(page.getPageData(), 0, BufferPool.getPageSize());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) this.f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // initialise new ArrayList to store modified pages
        ArrayList<Page> modifiedPages = new ArrayList<>();
        // get number of pages in this file
        int totalPages = this.numPages();

        // iterate through pages in file to find page with empty slot for tuple
        for (int currentPageNo = 0; currentPageNo < totalPages; currentPageNo++) {
            // get current page id
            HeapPageId pageId = new HeapPageId(this.getId(), currentPageNo);
            // get current page using pageid
            HeapPage currentHeapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);

            // check if current page has empty slots
            if (currentHeapPage.getNumEmptySlots() > 0) {
                // if current heap page has empty slot, insert tuple, once tuple inserted, tuple
                // properties updated
                currentHeapPage.insertTuple(t);
                // update list of modified page
                modifiedPages.add(currentHeapPage);
                break;
            }
        }

        // if no pages with empty slots, create new page
        if (modifiedPages.isEmpty()) {
            // create new heap page
            HeapPage newHeapPage = new HeapPage(new HeapPageId(this.getId(), totalPages),
                    HeapPage.createEmptyPageData());
            // insert tuple to new heap page
            newHeapPage.insertTuple(t);
            // push new heap page into disk
            this.writePage(newHeapPage);
            // add heap page to modified pages
            modifiedPages.add(newHeapPage);
        }
        return modifiedPages; // return list of pages that is changed
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // initiate new ArrayList to store modified pages
        ArrayList<Page> modifiedPages = new ArrayList<>();
        // get page id of tuple
        PageId pageId = t.getRecordId().getPageId();
        // get heap page using page id
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        // remove tuple from heap page
        heapPage.deleteTuple(t);
        // add heap page to modified pages
        modifiedPages.add(heapPage);
        return modifiedPages; // return modified pages
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private Integer pageCursor;
        private Iterator<Tuple> tupleIter;
        private final TransactionId transactionId;
        private final int numPages;
        private final int tableId;

        public HeapFileIterator(TransactionId transactionId) {
            this.pageCursor = null;
            this.tupleIter = null;
            this.transactionId = transactionId;
            this.tableId = getId();
            this.numPages = numPages();

        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageCursor = 0;
            tupleIter = getTupleIter(pageCursor);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (pageCursor != null) {
                while (pageCursor < numPages - 1) {
                    if (tupleIter.hasNext()) {
                        return true;

                    } else {
                        pageCursor += 1;
                        tupleIter = getTupleIter(pageCursor);
                    }
                }
                return tupleIter.hasNext();
            } else {
                return false;
            }

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                return tupleIter.next();
            }

            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pageCursor = null;
            tupleIter = null;

        }

        private Iterator<Tuple> getTupleIter(Integer pageNumber) throws TransactionAbortedException, DbException {
            PageId page_id = new HeapPageId(tableId, pageNumber);
            return ((HeapPage) Database.getBufferPool().getPage(transactionId, page_id, Permissions.READ_ONLY))
                    .iterator();

        }
    }

}
