package com.github.rtempleton.phoenixudfs;

import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PTimestamp;
import org.joda.time.DateTime;

@BuiltInFunction(name=Timemachine.FUNC_NAME,  args={@Argument(allowedTypes={PTimestamp.class}), @Argument(allowedTypes={PInteger.class}), @Argument(allowedTypes={PInteger.class})} )
public class Timemachine extends ScalarFunction {
	
	public static final String FUNC_NAME = "Timemachine";

	public Timemachine() {
	}

	public Timemachine(List<Expression> children) {
		super(children);
	}

	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		
		Timestamp t = null;
		Integer b = null;
		Integer c = null;
		
		for (int i=0; i<3; i++) {
			Expression arg = getChildren().get(i);
			if(!arg.evaluate(tuple, ptr))
				return false;
		
			switch(i) {
			case 0:
				t = (Timestamp) PTimestamp.INSTANCE.toObject(ptr);
				break;
			case 1:
				b = (Integer) PInteger.INSTANCE.toObject(ptr);
				break;
			case 2:
				c = (Integer) PInteger.INSTANCE.toObject(ptr);
				break;
			default:
				return false;
			}
		}
		
		
		DateTime dt = new DateTime(t.getTime());
		dt = dt.withSecondOfMinute(0);
		dt = dt.withMillisOfSecond(0);
		dt = dt.plusMinutes(-(dt.getMinuteOfHour()%Math.abs(b)) + c*Math.abs(b));
		ptr.set(PTimestamp.INSTANCE.toBytes(new Timestamp(dt.getMillis())));
		
		return true;
	}

	public PDataType getDataType() {
		return PTimestamp.INSTANCE;
	}

	public String getName() {
		return FUNC_NAME;
	}

}
