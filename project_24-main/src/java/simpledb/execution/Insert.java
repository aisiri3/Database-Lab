package simpledb.execution;

import java.io.IOException;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private TransactionId transactionId;
    private OpIterator opIter;
    private int tableId;
    private boolean tupleCalled;
    private BufferPool bufferPool;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *                The transaction running the insert.
     * @param child
     *                The child operator from which to read tuples to be inserted.
     * @param tableId
     *                The table in which to insert tuples.
     * @throws DbException
     *                     if TupleDesc of child differs from table into which we
     *                     are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.transactionId = t;
        this.opIter = child;
        this.tableId = tableId;
        this.tupleCalled = false;
        this.bufferPool = new BufferPool(1);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        opIter.open();
        this.tupleCalled = false; // new tuple called, so cannot be more than once
    }

    public void close() {
        // super.close();
        opIter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.opIter.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.tupleCalled)
            return null;

        Tuple resultTuple = new Tuple(new TupleDesc(new Type[] { Type.INT_TYPE }));
        int insertCount = 0;

        try {
            while (this.opIter.hasNext()) {
                Tuple tuple = this.opIter.next();
                insertCount++;
                Database.getBufferPool().insertTuple(this.transactionId, this.tableId, tuple);
            }
        } catch (IOException e) {
            throw new DbException("wrong");
        }

        // System.out.println(empty.numPages());
        resultTuple.setField(0, new IntField(insertCount));

        this.tupleCalled = true;
        return resultTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] opIter = new OpIterator[1];
        opIter[0] = this.opIter;
        return opIter;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        OpIterator[] opIter = new OpIterator[1];
        opIter[0] = this.opIter;
        children = opIter;
    }
}
