package flink.cep.events;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: hugo.zxh
 * @date: 2023/10/09 14:28
 */
public class OrderEvent {
    private String userId;
    private String orderId;
    private String eventType;
    private Long timestamp;


    public String getUserId() {
        return userId;
    }

    public String getOrderId() {
        return orderId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getEventType() {
        return eventType;
    }

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String eventType, Long timestamp) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                "orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
