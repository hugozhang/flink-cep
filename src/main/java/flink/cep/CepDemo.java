/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.cep;

import java.util.List;
import java.util.Map;

import flink.cep.events.Warning;
import flink.cep.source.LoginEventSource;
import flink.cep.source.MySQLDataSource;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.cep.events.LoginEvent;


// Different contiguity choices, simple pattern example

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class CepDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);


        //输入数据，提取事件时间
        KeyedStream<LoginEvent, Integer> loginEventKeyedStream = env.addSource(new MySQLDataSource())
//                fromElements(
//                new LoginEvent(1, "success", 1575600181000L),
//                new LoginEvent(2, "fail1", 1575600182000L),
//                new LoginEvent(2, "fail2", 1575600183000L),
//                new LoginEvent(3, "fail1", 1575600184000L),
//                new LoginEvent(3, "fail2", 1575600189000L)
//        )
                .assignTimestampsAndWatermarks(
                //注册watermark方法和旧版稍有不同
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((event,timestamp)-> event.getEventTime()))
                .keyBy(new KeySelector<LoginEvent, Integer>() {
                    @Override
                    public Integer getKey(LoginEvent loginEvent) throws Exception {
                        return loginEvent.getUserId();
                    }
                });

        //定义Pattern
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).<LoginEvent>next("next").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).within(Time.seconds(5));

        //检测模式
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventKeyedStream, pattern);

        //侧输出标志
        OutputTag<LoginEvent> outputTag = new OutputTag<LoginEvent>("timeout") {};

        //process方式提取数据
        SingleOutputStreamOperator<Warning> process = patternStream.process(new MyPatternProcessFunction(outputTag));
        process.print("process login failed twice");
        //提取超时数据
        process.getSideOutput(outputTag).print("process timeout");

        //select方式提取数据
//        SingleOutputStreamOperator<Warning> outputStreamOperator = patternStream
//                .select(
//                        outputTag,
//                        new PatternTimeoutFunction<LoginEvent, LoginEvent>() {
//                    @Override
//                    public LoginEvent timeout(Map<String, List<LoginEvent>> map, long l) throws Exception {
//
//                        return map.get("begin").iterator().next();
//                    }
//                },
//                        new PatternSelectFunction<LoginEvent, Warning>() {
//                    @Override
//                    public Warning select(Map<String, List<LoginEvent>> map) throws Exception {
//                        LoginEvent begin = map.get("begin").iterator().next();
//                        LoginEvent next = map.get("next").iterator().next();
//
//                        return new Warning(begin.getUserId(), begin.getEventTime(), next.getEventTime(), "Login failed twice");
//                    }
//                });

        //提取超时的数据
//        DataStream<LoginEvent> timeoutDataStream = outputStreamOperator.getSideOutput(outputTag);
//        timeoutDataStream.print("timeout");

        //提取匹配数据
//        outputStreamOperator.print("Login failed twice");


        env.execute();
    }


    //TimedOutPartialMatchHandler提供了另外的processTimedOutMatch方法，这个方法对每个超时的部分匹配都会调用。
    static class MyPatternProcessFunction extends PatternProcessFunction<LoginEvent, Warning> implements TimedOutPartialMatchHandler<LoginEvent> {
        private OutputTag<LoginEvent> outputTag;

        public MyPatternProcessFunction(OutputTag<LoginEvent> outputTag) {
            this.outputTag = outputTag;
        }

        @Override
        public void processMatch(Map<String, List<LoginEvent>> map, Context context, Collector<Warning> collector) throws Exception {
            LoginEvent begin = map.get("begin").iterator().next();
            LoginEvent next = map.get("next").iterator().next();

            collector.collect(new Warning(begin.getUserId(), begin.getEventType(), next.getEventTime(), "Login failed twice"));
        }

        @Override
        public void processTimedOutMatch(Map<String, List<LoginEvent>> map, Context context) throws Exception {
            context.output(outputTag,map.get("begin").iterator().next());
        }
    }
}


