package org.apache.flink.quickstart;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class SavepointTest {

	public static void main (String [] args) throws Exception{
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.enableCheckpointing(1000);
		
		env.setStateBackend(new MemoryStateBackend());
		
		DataStream<String> stream = env.socketTextStream("localhost", 8888)
				.map(new MapFunction<String, String>() {

					private static final long serialVersionUID = 1519592231733654448L;

					@Override
					public String map(String arg) throws Exception {
						return arg;
					}
				})
				.timeWindowAll(Time.of(5, TimeUnit.SECONDS))
				.apply(new RichAllWindowFunction<String, String, TimeWindow>() {
					
					private static final long serialVersionUID = 1L;
					private ValueStateDescriptor<Integer> descriptor;
					
					@Override
					public void apply(TimeWindow window,
							Iterable<String> values, Collector<String> out)
							throws Exception {
						Integer value = getRuntimeContext().getState(this.descriptor).value();
						value++;
						getRuntimeContext().getState(this.descriptor).update(value);
						System.out.println("Status value "  + value);
						out.collect(String.valueOf(value));						
					}
					@Override
					public void open (Configuration config){
						this.descriptor =  new ValueStateDescriptor<>(
								"last-result", new TypeHint<Integer>() {
								}.getTypeInfo(), new  Integer(0));
					}
				});
		
		
		stream.print();

	
		env.execute("Savepoint example");
	}
}
