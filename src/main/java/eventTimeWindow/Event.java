package eventTimeWindow;

/*
事件数据类
 */
public class Event {
    private String key;
    private long timestamp;
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

    public void setValue(String value) {
        this.value = value;
    }
}
