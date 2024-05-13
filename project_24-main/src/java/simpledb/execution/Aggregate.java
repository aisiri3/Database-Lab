package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private OpIterator opIter;
    private int gFieldIdx;
    private int aFieldIdx;
    private Aggregator.Op aop;
    private Aggregator newAggregator;
    private OpIterator newOpIter;

    private Type groupType;
    private Type aggregateType;
    private String groupName;
    private String aggregateName;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.opIter = child;
        this.gFieldIdx = gfield;
        this.aFieldIdx = afield;
        this.aop = aop;

        // Get types and names
        TupleDesc td = this.opIter.getTupleDesc();
        this.groupType = (gfield == Aggregator.NO_GROUPING) ? null : td.getFieldType(gfield);
        this.aggregateType = td.getFieldType(afield);
        this.groupName = (gfield == Aggregator.NO_GROUPING) ? null : td.getFieldName(gfield);
        this.aggregateName = td.getFieldName(afield);

        // Initialize aggregator
        this.newAggregator = (aggregateType == Type.INT_TYPE)
                ? new IntegerAggregator(gfield, groupType, afield, aop)
                : new StringAggregator(gfield, groupType, afield, aop);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.gFieldIdx;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        // some code goes here
        return this.groupName;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.aFieldIdx;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return this.aggregateName;
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    // public void open() throws NoSuchElementException, DbException,
    // TransactionAbortedException {
    // super.open();
    // this.opIter.open();
    // while (this.opIter.hasNext()) {
    // this.newAggregator.mergeTupleIntoGroup(this.opIter.next());
    // }

    // // init new agg iter
    // this.newOpIter = this.newAggregator.iterator();
    // this.newOpIter.open();
    // }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        try {
            super.open();
            this.opIter.open();
            while (this.opIter.hasNext()) {
                this.newAggregator.mergeTupleIntoGroup(this.opIter.next());
            }
            this.newOpIter = this.newAggregator.iterator();
            this.newOpIter.open();
        } catch (Exception e) {
            throw new DbException("Error opening Aggregate operator: " + e.getMessage());
        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.newOpIter.hasNext()) {
            return this.newOpIter.next();
        }

        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.newOpIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String aggName = this.aop.toString() + "(" + this.aggregateName + ")";
        if (this.gFieldIdx == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { aggName });
        } else {
            return new TupleDesc(new Type[] { this.groupType, Type.INT_TYPE },
                    new String[] { this.groupName, aggName });
        }
    }

    public void close() {
        // some code goes here
        this.newOpIter.close();
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] opIter = new OpIterator[1];
        opIter[0] = this.newOpIter;
        return opIter;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        OpIterator[] opIter = new OpIterator[1];
        opIter[0] = this.newOpIter;
        children = opIter;
    }

}
