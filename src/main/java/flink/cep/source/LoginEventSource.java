package flink.cep.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import flink.cep.events.LoginEvent;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: hugo.zxh
 * @date: 2023/10/11 14:34
 */
public class LoginEventSource extends RichSourceFunction<LoginEvent> {

    private boolean running = true;

    private String[] event = new String[] {"success","fail1","fail2"};

    @Override
    public void run(SourceContext<LoginEvent> ctx) throws Exception {
        while (this.running) {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            LoginEvent loginEvent = new LoginEvent(random.nextInt(1, 3), event[random.nextInt(0,2)], System.currentTimeMillis() + random.nextInt(1000, 5000));
            ctx.collect(loginEvent);
            System.out.println(loginEvent);
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
