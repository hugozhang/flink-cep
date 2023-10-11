package flink.cep;

import flink.cep.events.LoginEvent;
import flink.cep.source.MySQLDataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: hugo.zxh
 * @date: 2023/10/11 16:37
 */
public class RichSourceFunctionDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度为2
        env.setParallelism(2);

        DataStream<LoginEvent> dataStream = env.addSource(new MySQLDataSource());
        dataStream.print();

        env.execute("Customize DataSource demo : RichSourceFunction");
    }

}
