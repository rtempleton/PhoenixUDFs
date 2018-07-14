package com.github.rtempleton.phoenixudfs;

import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PTimestamp;
import org.joda.time.DateTime;

@BuiltInFunction(name=AddTime.FUNC_NAME,  args={@Argument(allowedTypes={PTimestamp.class}), @Argument(allowedTypes={PInteger.class}), @Argument(allowedTypes={PChar.class}),} )
public class AddTime extends ScalarFunction {

	public static final String FUNC_NAME = "AddTime";
	
	public AddTime() {
		// TODO Auto-generated constructor stub
	}

	public AddTime(List<Expression> children) {
		super(children);
		// TODO Auto-generated constructor stub
	}

	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		
		Timestamp t = null;
		Integer a = null;
		String u = null;
		
		for (int i=0; i<3; i++) {
			Expression arg = getChildren().get(i);
			if(!arg.evaluate(tuple, ptr))
				return false;
		
			switch(i) {
			case 0:
				t = (Timestamp) PTimestamp.INSTANCE.toObject(ptr);
				break;
			case 1:
				a = (Integer) PInteger.INSTANCE.toObject(ptr);
				break;
			case 2:
				u = new String(ptr.copyBytes());
				break;
			default:
				return true;
			}
		}
		
		DateTime d = new DateTime(t.getTime());
		switch(u) {
		case "y":
			d = d.plusYears(a);
			break;
		case "M":
			d = d.plusMonths(a);
			break;
		case "d":
			d = d.plusDays(a);
			break;
		case "h":
			d = d.plusHours(a);
			break;
		case "m":
			d = d.plusMinutes(a);
			break;
		case "s":
			d = d.plusSeconds(a);
			break;
		default:
			return true;
		
		}
		
		Timestamp x = new Timestamp(d.getMillis());
		ptr.set(PTimestamp.INSTANCE.toBytes(x));
		return true;
		
	}

	public PDataType getDataType() {
		return PTimestamp.INSTANCE;
	}

	@Override
	public String getName() {
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
