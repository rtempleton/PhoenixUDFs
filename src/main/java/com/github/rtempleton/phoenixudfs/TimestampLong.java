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
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;

@BuiltInFunction(name=TimestampLong.FUNC_NAME,  args={@Argument(allowedTypes={PTimestamp.class})})
public class TimestampLong extends ScalarFunction {

	public static final String FUNC_NAME = "TimestampLong";
	
	public TimestampLong() {
	}

	public TimestampLong(List<Expression> children) {
		super(children);
	}

	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		
		Expression arg = getChildren().get(0);
		if(!arg.evaluate(tuple, ptr))
			return false;
		
		Timestamp t = (Timestamp) PTimestamp.INSTANCE.toObject(ptr);
		ptr.set(PLong.INSTANCE.toBytes(t.getTime()));
		return true;
	}

	@Override
	public PDataType getDataType() {
		return PLong.INSTANCE;
	}

	@Override
	public String getName() {
		return FUNC_NAME;
	}

}
