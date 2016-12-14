package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private int[] histogram;
	private int buckets;
	private int min;
	private int max;
	private double bucketRange;
	private int numberTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.histogram = new int[buckets];
    	Arrays.fill(histogram, 0);
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	this.bucketRange = 1.0 * (max - min + 1) / buckets;
    	this.numberTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int bucketNumber = getBucketNumber(v);
    	
    	histogram[bucketNumber] += 1;
    	numberTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int bucketNumber = getBucketNumber(v);
    	double selectivity = 0;
    	
    	if (op.equals(Predicate.Op.EQUALS)) {
    		if (isOutsideRange(v)) return 0;
    		selectivity = 1.0 * histogram[bucketNumber] / Math.max(1, Math.floor(bucketRange)) / numberTuples;
    	} else if (op.equals(Predicate.Op.NOT_EQUALS)) {
    		if (isOutsideRange(v)) return 1;
    		selectivity = 1 - (1.0 * histogram[bucketNumber] / Math.max(1, Math.floor(bucketRange)) / numberTuples);
    	} else if (op.equals(Predicate.Op.GREATER_THAN)) {
    		selectivity = getRangeSelectivity(bucketNumber, v, true, false);
    	} else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
    		selectivity = getRangeSelectivity(bucketNumber, v, true, true);
    	} else if (op.equals(Predicate.Op.LESS_THAN)) {
    		selectivity = getRangeSelectivity(bucketNumber, v, false, false);
    	} else if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
    		selectivity = getRangeSelectivity(bucketNumber, v, false, true);
    	}
    	
    	return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        for (int i = 0; i < histogram.length; i++) {
        	stringBuilder.append(histogram[i] + " ");
        }
        stringBuilder.append("]");
        
        return stringBuilder.toString();
    }
    
    private double getRangeSelectivity(int bucketNumber, int v, boolean greaterThan, boolean inclusiveRange) {
    	if ((greaterThan && v < min) || (!greaterThan && v > max)) {
    		return 1;
    	} else if ((greaterThan && v > max) || (!greaterThan && v < min)) {
    		return 0;
    	}
    	
    	double bucketFraction = 1.0 * histogram[bucketNumber] / numberTuples;
    	int inclusive = inclusiveRange ? 1 : 0;
    	
    	double selectivity = 0;
    	selectivity += getBucketPartialSelectivity(bucketNumber, v, greaterThan, inclusive, bucketFraction);
    	selectivity += getRemainingBucketSelectivity(bucketNumber, greaterThan);
		
		return selectivity;
    }
    
    private double getBucketPartialSelectivity(int bucketNumber, int v, boolean greaterThan, int inclusive, double bucketFraction) {
    	double bucketPartialWidth = 0;
    	if (greaterThan) {
    		bucketPartialWidth = Math.floor((bucketNumber + 1) * bucketRange + min) - v + inclusive;
    	} else {
    		bucketPartialWidth = v - Math.floor(bucketNumber * bucketRange + min) + inclusive;
    	}
		return bucketFraction * (bucketPartialWidth / Math.max(1, Math.floor(bucketRange)));
    }
    
    private double getRemainingBucketSelectivity(int bucketNumber, boolean greaterThan) {
    	double selectivity = 0;
    	
    	if (greaterThan) {
			for (int i = bucketNumber + 1; i < buckets; i ++) {
				selectivity += 1.0 * histogram[i] / numberTuples;
			}
		} else {
			for (int i = bucketNumber - 1; i >= 0; i--) {
				selectivity += 1.0 * histogram[i] / numberTuples;
			}
		}
    	
    	return selectivity;
    }
    
    /** Assumes that the input value is in range */
    private int getBucketNumber(int value) {
    	return (int) ((value - min) / bucketRange);
    }
    
    private boolean isOutsideRange(int value) {
    	return value > max || value < min;
    }
}
