package flink.cep.events;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: hugo.zxh
 * @date: 2023/10/09 10:09
 */
public class Warning extends LoginEvent {


    private String warning;

    public Warning(int userId, String eventType, long eventTime,String warning) {
        super(userId, eventType, eventTime);
        this.warning = warning;
    }
}
