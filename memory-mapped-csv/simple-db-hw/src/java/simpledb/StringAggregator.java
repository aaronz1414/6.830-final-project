package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op operator;
    
    private Map<Field, Integer> groupSizes;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if (!Op.COUNT.equals(what)) throw new IllegalArgumentException();
    	
    	this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.operator = what;
        this.groupSizes = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field key = new IntField(0);
    	
    	if (groupByField != Aggregator.NO_GROUPING) {
    		key = tup.getField(groupByField);
    	}
    	
    	updateGroupSize(key);
    }
    
    private void updateGroupSize(Field key) {
    	if (groupSizes.containsKey(key)) {
    		groupSizes.put(key, groupSizes.get(key) + 1);
    	} else {
    		groupSizes.put(key, 1);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	List<Tuple> tuples = new ArrayList<>();
        TupleDesc tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {null});
        
        if (groupByField != Aggregator.NO_GROUPING) {
        	tupleDesc = new TupleDesc(new Type[] {groupByFieldType, Type.INT_TYPE}, new String[] {null, null});
        }
        
        for (Field key : groupSizes.keySet()) {
        	
        	Tuple newTuple = new Tuple(tupleDesc);
        	
        	if (groupByField == Aggregator.NO_GROUPING) {
        		newTuple.setField(0, new IntField(groupSizes.get(key)));
        	} else {
        		newTuple.setField(0, key);
        		newTuple.setField(1, new IntField(groupSizes.get(key)));
        	}
        	
        	tuples.add(newTuple);
        }
        
        return new TupleIterator(tupleDesc, tuples);
    }

}
