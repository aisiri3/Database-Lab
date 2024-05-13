package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.storage.Field;
import simpledb.storage.IntField;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gFieldIdx;
    private Type gFieldType;
    private int aFieldIdx;
    private Op op;
    private HashMap<Field, Integer> fields;
    private HashMap<Field, Integer> aggregates;
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gFieldIdx = gbfield;
        this.gFieldType = gbfieldtype;
        this.aFieldIdx = afield;
        this.op = what;
        this.fields = new HashMap<>();
        this.aggregates = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here Field grpByField = (this.gFieldIdx !=
        // Aggregator.NO_GROUPING) ? tup.getField(this.gFieldIdx) : null;
        Field grpByField = (this.gFieldIdx != Aggregator.NO_GROUPING) ? tup.getField(this.gFieldIdx) : null;

        int fieldIndex = 0;
        if (this.fields.get(grpByField) != null) {
            fieldIndex = this.fields.get(grpByField);
        }
        this.fields.put(grpByField, fieldIndex + 1);

        int tupleValue = Integer.parseInt(tup.getField(this.aFieldIdx).toString());
        int curFieldValue = this.fields.get(grpByField);
        int curAggValue = 0;

        // Does it exist? If yes, give it the first default value, otherwise slide that
        // existing value from memory in.
        if (this.aggregates.get(grpByField) == null) {
            if (this.op.toString() == "max") {
                curAggValue = tupleValue;
            } else if (this.op.toString() == "min") {
                curAggValue = tupleValue;
            } else if (this.op.toString() == "sum") {
                curAggValue = 0;
            } else if (this.op.toString() == "avg") {
                curAggValue = 0;
            } else if (this.op.toString() == "count") {
                curAggValue = 0;
            }
        } else {
            curAggValue = this.aggregates.get(grpByField);
        }

        int updAggValue = curAggValue;

        // Perform new operation based on operator
        if (this.op.toString() == "sum") {
            updAggValue = curAggValue + tupleValue;
        } else if (this.op.toString() == "avg") {
            updAggValue = curAggValue + tupleValue;
        } else if (this.op.toString() == "max") {
            updAggValue = Math.max(curAggValue, tupleValue);
        } else if (this.op.toString() == "min") {
            updAggValue = Math.min(curAggValue, tupleValue);
        } else if (this.op.toString() == "count") {
            updAggValue = curAggValue + 1;
        }
        ;

        this.aggregates.put(grpByField, updAggValue);

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuplesArray = new ArrayList<Tuple>();

        TupleDesc newTupleDesc;
        if (this.gFieldIdx != Aggregator.NO_GROUPING) {
            newTupleDesc = new TupleDesc(new Type[] { this.gFieldType, Type.INT_TYPE });
        } else {
            newTupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
        }

        for (Map.Entry<Field, Integer> entry : this.aggregates.entrySet()) {
            Tuple newTuple = new Tuple(newTupleDesc);

            Field finalAggkey = entry.getKey();
            int finalAggVal = 0;
            if (this.op.toString() == "avg") {
                finalAggVal = entry.getValue() / this.fields.get(entry.getKey());
            } else if (this.op.toString() == "sum") {
                finalAggVal = entry.getValue();
            } else if (this.op.toString() == "avg") {
                finalAggVal = entry.getValue();
            } else if (this.op.toString() == "max") {
                finalAggVal = entry.getValue();
            } else if (this.op.toString() == "min") {
                finalAggVal = entry.getValue();
            } else if (this.op.toString() == "count") {
                finalAggVal = entry.getValue();
            }
            ;

            if (this.gFieldIdx != Aggregator.NO_GROUPING) {
                newTuple.setField(0, finalAggkey);
                newTuple.setField(1, new IntField(finalAggVal));
            } else {
                newTuple.setField(0, new IntField(finalAggVal));
            }
            tuplesArray.add(newTuple);
        }
        return new TupleIterator(newTupleDesc, tuplesArray);
    }

}
