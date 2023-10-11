package flink.cep;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: hugo.zxh
 * @date: 2023/10/09 14:27
 */
import flink.cep.events.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class OrderTimeoutDetect {

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> dataStream = env.readTextFile("data/OrderLog.csv");

        DataStream<OrderEvent> orderDataStream = dataStream
                .filter(line -> line.split(",").length >= 4)
                .map(line -> {
                    String[] s = line.split(",");
                    return new OrderEvent(s[0], s[1], s[2], Long.valueOf(s[3]) * 1000);})
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        Pattern<OrderEvent, OrderEvent> payPattern =
                Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "pay".equals(orderEvent.getEventType());
                    }
                }).within(Time.minutes(1));

        PatternStream<OrderEvent> patternStream =
                CEP.pattern(orderDataStream.keyBy(OrderEvent::getOrderId), payPattern);

        OutputTag<Tuple4<String, Long, Long, String>> outputTag =
                new OutputTag<Tuple4<String, Long, Long, String>>("pay-timeout") {};

        SingleOutputStreamOperator<Tuple4<String, Long, Long, String>> select =
                patternStream.select(outputTag, new TimeoutSelectFunction(), new MyPatternSelectFunction());

        select.print("pay");

        select.getSideOutput(outputTag).print("timeout");

        env.execute("Table SQL");

        System.out.println("耗时: " + (System.currentTimeMillis() - start) / 1000);

    }

    public static class TimeoutSelectFunction
            implements PatternTimeoutFunction<OrderEvent, Tuple4<String, Long, Long, String>> {

        @Override
        public Tuple4<String, Long, Long, String> timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            OrderEvent create = map.get("create").iterator().next();
            return new Tuple4<String, Long, Long, String>(create.getOrderId(), create.getTimestamp(), l, "timeout: " + l);
        }
    }

    public static class MyPatternSelectFunction
            implements PatternSelectFunction<OrderEvent, Tuple4<String, Long, Long, String>> {

        @Override
        public Tuple4<String, Long, Long, String> select(Map<String, List<OrderEvent>> map) throws Exception {
            OrderEvent create = map.get("create").iterator().next();
            OrderEvent pay = map.get("pay").get(0);
            return new Tuple4<String, Long, Long, String>(create.getOrderId(), create.getTimestamp(),
                    pay.getTimestamp(), "payed");
        }
    }
}

