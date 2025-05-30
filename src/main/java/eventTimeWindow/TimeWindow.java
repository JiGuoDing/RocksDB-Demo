package eventTimeWindow;

/*
时间窗口
 */
public class TimeWindow {
    private long start;
    private long end;

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "TimeWindow{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}
