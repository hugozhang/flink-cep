package flink.cep.source;

import flink.cep.events.LoginEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: hugo.zxh
 * @date: 2023/10/11 14:58
 */
public class MySQLDataSource extends RichSourceFunction<LoginEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLDataSource.class);


    private Connection connection = null;

    private PreparedStatement preparedStatement = null;

    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        if(null==connection) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://192.168.90.57:3306/flink_cep?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&useSSL=false&noAccessToProcedureBodies=true&serverTimezone=GMT%2B8&connectionCollation=utf8mb4_general_ci&rewriteBatchedStatements=true", "root", "hmap");
        }

        if(null==preparedStatement) {
            preparedStatement = connection.prepareStatement("select user_id, event_type,event_time from login_event");
        }
    }

    /**
     * 释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();

        if(null!=preparedStatement) {
            try {
                preparedStatement.close();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }

        if(null==connection) {
            connection.close();
        }
    }

    @Override
    public void run(SourceContext<LoginEvent> ctx) throws Exception {
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next() && isRunning) {

            int userId = resultSet.getInt("user_id");
            String eventType = resultSet.getString("event_type");
            long eventTime = resultSet.getDate("event_time").getTime();
            LoginEvent loginEvent = new LoginEvent(userId,eventType,eventTime);
            LOG.info("Login Event : {}.",loginEvent);
            ctx.collect(loginEvent);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
