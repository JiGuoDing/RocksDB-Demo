package eventTimeWindow;

import java.util.Collection;
import java.util.Collections;

public class WindowAssigner {
    private long windowSize;

    public WindowAssigner(long windowSize) {
        this.windowSize = windowSize;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(long windowSize) {
        this.windowSize = windowSize;
    }

    public Collection<TimeWindow> assignWindows(long timestamp){
        /*
        滚动窗口
         */
        long start = timestamp - (timestamp % windowSize);
        long end = start + windowSize;
        return Collections.singletonList(new TimeWindow(start, end));
    }
}
