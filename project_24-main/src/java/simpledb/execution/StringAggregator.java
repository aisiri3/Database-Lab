package simpledb.execution;

import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import java.util.ArrayList;
import java.util.Map;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.storage.Field;
import simpledb.storage.IntField;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private int gFieldIdx;
    private Type gFieldType;
    private int aFieldIdx;
    private Op op;
    private HashMap<Field, Integer> fields;
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gFieldIdx = gbfield;
        this.gFieldType = gbfieldtype;
        this.aFieldIdx = afield;
        this.op = what;
        this.fields = new HashMap<>();

        if (what == null)
            throw new IllegalArgumentException();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field grpByField = (this.gFieldIdx != Aggregator.NO_GROUPING) ? tup.getField(this.gFieldIdx) : null;

        int fieldIndex = 0;
        if (this.fields.get(grpByField) != null) {
            fieldIndex = this.fields.get(grpByField);
        }
        this.fields.put(grpByField, fieldIndex + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuplesArray = new ArrayList<Tuple>();

        TupleDesc newTupleDesc;
        if (this.gFieldIdx != Aggregator.NO_GROUPING) {
            newTupleDesc = new TupleDesc(new Type[] { this.gFieldType, Type.INT_TYPE });
        } else {
            newTupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
        }

        for (Map.Entry<Field, Integer> entry : this.fields.entrySet()) {
            Tuple newTuple = new Tuple(newTupleDesc);

            if (this.gFieldIdx != Aggregator.NO_GROUPING) {
                newTuple.setField(0, entry.getKey());
                newTuple.setField(1, new IntField((int) entry.getValue()));
            } else {
                newTuple.setField(0, new IntField((int) entry.getValue()));
            }
            tuplesArray.add(newTuple);
        }
        return new TupleIterator(newTupleDesc, tuplesArray);
    }

}
