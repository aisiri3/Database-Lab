package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId transactionId;
    private OpIterator opIter;
    private boolean tupleCalled;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *              The transaction this delete runs in
     * @param child
     *              The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId = t;
        this.opIter = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        opIter.open();
        this.tupleCalled = false;
    }

    public void close() {
        opIter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.opIter.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.tupleCalled)
            return null;

        Tuple resultTuple = new Tuple(new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "" }));
        int insertCount = 0;

        try {
            while (this.opIter.hasNext()) {
                Tuple tuple = this.opIter.next();
                insertCount++;
                Database.getBufferPool().deleteTuple(this.transactionId, tuple);
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
        // some code goes here
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
