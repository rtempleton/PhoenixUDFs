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

@BuiltInFunction(name=LongTimestamp.FUNC_NAME,  args={@Argument(allowedTypes={PLong.class})})
public class LongTimestamp extends ScalarFunction{

	public static final String FUNC_NAME = "LongTimestamp";
	
	public LongTimestamp() {
	}
	
	public LongTimestamp(List<Expression> children) {
		super(children);
	}
	
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		
		Expression arg = getChildren().get(0);
		if(!arg.evaluate(tuple, ptr))
			return false;
		
		Long l = (Long) PLong.INSTANCE.toObject(ptr);
		ptr.set(PTimestamp.INSTANCE.toBytes(new Timestamp(l)));
		return true;
	}
	
	public PDataType getDataType() {
		return PTimestamp.INSTANCE;
	}

	public String getName() {
		return FUNC_NAME;
	}

}
