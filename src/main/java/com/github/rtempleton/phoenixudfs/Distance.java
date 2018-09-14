package com.github.rtempleton.phoenixudfs;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PTimestamp;

@BuiltInFunction(name=AddTime.FUNC_NAME,  args={@Argument(allowedTypes={PDouble.class}), @Argument(allowedTypes={PDouble.class}), @Argument(allowedTypes={PDouble.class}), @Argument(allowedTypes={PDouble.class})} )
public class Distance extends ScalarFunction {
	
	public static final String FUNC_NAME = "Distance";

	public Distance() {
		// TODO Auto-generated constructor stub
	}

	public Distance(List<Expression> children) {
		super(children);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		
		Double lat1 = null;
		Double lon1 = null;
		Double lat2 = null;
		Double lon2 = null;
		
		for (int i=0; i<4; i++) {
			Expression arg = getChildren().get(i);
			if(!arg.evaluate(tuple, ptr))
				return false;
		
			switch(i) {
			case 0:
				lat1 = (Double) PDouble.INSTANCE.toObject(ptr);
				break;
			case 1:
				lon1 = (Double) PDouble.INSTANCE.toObject(ptr);
				break;
			case 2:
				lat2 = (Double) PDouble.INSTANCE.toObject(ptr);
				break;
			case 3:
				lon2 = (Double) PDouble.INSTANCE.toObject(ptr);
				break;
			default:
				return true;
			}
			
			
		}
		
		double dist = distance(lat1, lon1, lat2, lon2);
		ptr.set(PDouble.INSTANCE.toBytes(dist));
		return true;
	}
	
	private static double distance(double lat1, double lon1, double lat2, double lon2) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;
        return (dist);
 }

    private static double deg2rad(double deg) {
           return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad) {
           return (rad * 180 / Math.PI);
    }


	@Override
	public PDataType getDataType() {
		// TODO Auto-generated method stub
		return PDouble.INSTANCE;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return FUNC_NAME;
	}
	
	/**
     * Determines whether or not a function may be used to form the start/stop
     * key of a scan
     * 
     * @return the zero-based position of the argument to traverse into to look
     *         for a primary key column reference, or {@value #NO_TRAVERSAL} if
     *         the function cannot be used to form the scan key.
     */
    public int getKeyFormationTraversalIndex() {
        return NO_TRAVERSAL;
    }

    /**
     * Manufactures a KeyPart used to construct the KeyRange given a constant
     * and a comparison operator.
     * 
     * @param childPart
     *            the KeyPart formulated for the child expression at the
     *            {@link #getKeyFormationTraversalIndex()} position.
     * @return the KeyPart for constructing the KeyRange for this function.
     */
    public KeyPart newKeyPart(KeyPart childPart) {
        return null;
    }

    /**
     * Determines whether or not the result of the function invocation will be
     * ordered in the same way as the input to the function. Returning YES
     * enables an optimization to occur when a GROUP BY contains function
     * invocations using the leading PK column(s).
     * 
     * @return YES if the function invocation will always preserve order for the
     *         inputs versus the outputs and false otherwise, YES_IF_LAST if the
     *         function preserves order, but any further column reference would
     *         not continue to preserve order, and NO if the function does not
     *         preserve order.
     */
    public OrderPreserving preservesOrder() {
        return OrderPreserving.NO;
    }

}
