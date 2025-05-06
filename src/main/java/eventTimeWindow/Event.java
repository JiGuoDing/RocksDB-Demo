package eventTimeWindow;

/*
事件数据类
 */
public class Event {
    /*
    事件的键
     */
    private String key;
    /*
    事件的时间戳
     */
    private long timestamp;
    /*
    事件的值
     */
    private String value;

    public Event(String key, long timestamp, String value){
        this.key = key;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Event{" +
                "key='" + key + '\'' +
                ", timestamp=" + timestamp +
                ", value='" + value + '\'' +
                '}';
    }

    public void setValue(String value) {
        this.value = value;
    }
}
