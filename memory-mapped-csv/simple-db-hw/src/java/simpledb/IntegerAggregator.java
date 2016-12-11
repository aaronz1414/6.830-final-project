package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op operator;
    
    private Map<Field, Integer> groupSizes;
    private Map<Field, Integer> groupAggregates;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.operator = what;
        this.groupSizes = new HashMap<>();
        this.groupAggregates = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field key = new IntField(0);
    	
    	if (groupByField != Aggregator.NO_GROUPING) {
    		key = tup.getField(groupByField);
    	}
    	
    	updateGroupSize(key);
		updateAggregate(key, tup);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {null});
        
        if (groupByField != Aggregator.NO_GROUPING) {
        	tupleDesc = new TupleDesc(new Type[] {groupByFieldType, Type.INT_TYPE}, new String[] {null, null});
        }
        
        for (Field key : groupAggregates.keySet()) {
        	int aggregate = groupAggregates.get(key);
        	
        	if (Op.AVG.equals(operator)) {
        		aggregate = aggregate / groupSizes.get(key);
        	}
        	
        	Tuple newTuple = new Tuple(tupleDesc);
        	
        	if (groupByField == Aggregator.NO_GROUPING) {
        		newTuple.setField(0, new IntField(aggregate));
        	} else {
        		newTuple.setField(0, key);
        		newTuple.setField(1, new IntField(aggregate));
        	}
        	
        	tuples.add(newTuple);
        }
        
        return new TupleIterator(tupleDesc, tuples);
    }
    
    private void updateGroupSize(Field key) {
    	if (groupSizes.containsKey(key)) {
    		groupSizes.put(key, groupSizes.get(key) + 1);
    	} else {
    		groupSizes.put(key, 1);
    	}
    }
    
    private void updateAggregate(Field key, Tuple tuple) {
    	int value = ((IntField) tuple.getField(aggregateField)).getValue();
    	
    	if (Op.COUNT.equals(operator)) {
    		groupAggregates.put(key, groupSizes.get(key));
    	} else if (Op.MAX.equals(operator)) {
    		updateMax(key, value);
    	} else if (Op.MIN.equals(operator)) {
    		updateMin(key, value);
    	} else if (Op.AVG.equals(operator) || Op.SUM.equals(operator)) {
    		updateSum(key, value);
    	}
    }
    
    private void updateMax(Field key, int value) {
    	if (!groupAggregates.containsKey(key) || value > groupAggregates.get(key)) {
    		groupAggregates.put(key, value);
    	}
    }
    
    private void updateMin(Field key, int value) {
    	if (!groupAggregates.containsKey(key) || value < groupAggregates.get(key)) {
    		groupAggregates.put(key, value);
    	}
    }
    
    private void updateSum(Field key, int value) {
    	if (!groupAggregates.containsKey(key)) {
    		groupAggregates.put(key, value);
    	} else {
    		groupAggregates.put(key, groupAggregates.get(key) + value);
    	}
    }

}
