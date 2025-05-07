package eventTimeWindow;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WindowManager {
    private final Map<String, Map<TimeWindow, List<Event>>> windows = new HashMap<>();
    private final WindowAssigner windowAssigner;
    private long currentWatermark = Long.MIN_VALUE;
    private static final Logger logger = LoggerFactory.getLogger(WindowManager.class);
    private StateBackend stateBackend;

    public WindowManager(long windowSize) throws RocksDBException {
        this.windowAssigner = new WindowAssigner(windowSize);
        this.stateBackend = new StateBackend("data/rocksdb");
        initTestData();
    }

    private void initTestData() throws RocksDBException {
        byte[] largeValue = new byte[128 * 1024 * 1024];
        Arrays.fill(largeValue, (byte)1);
        for (int i = 0; i < 100; i++) {
            stateBackend.putState("key_" + i, largeValue);
        }
    }

    public Map<String, Map<TimeWindow, List<Event>>> getWindows() {
        return windows;
    }

    public WindowAssigner getWindowAssigner() {
        return windowAssigner;
    }

    public long getCurrentWatermark() {
        return currentWatermark;
    }

    public void setCurrentWatermark(long currentWatermark) {
        this.currentWatermark = currentWatermark;
    }

    public StateBackend getStateBackend() {
        return stateBackend;
    }

    public void setStateBackend(StateBackend stateBackend) {
        this.stateBackend = stateBackend;
    }

    public void prefetchForEvent(Event event) throws RocksDBException {
        /*
        预测下一个窗口可能访问的key
         */
        List<String> keysToPrefetch = Arrays.asList(
                "key_" + (event.getTimestamp() % 10000),
                "key_" + ((event.getTimestamp() + this.windowAssigner.getWindowSize()) % 10000)
        );
        stateBackend.prefetch(keysToPrefetch);
    }

    /*
    为指定的 n 个 event 预取状态
     */
    public void prefetchStateForEvent(List<Event> events) {
        List<String> keysToPrefetch = new ArrayList<>();

        events.forEach(event -> keysToPrefetch.add(event.getKey()));
        stateBackend.prefetch(keysToPrefetch);
    }

    public void processEvent(Event event){
        /*
        为事件分配窗口
         */
        Collection<TimeWindow> assignedWindows = windowAssigner.assignWindows(event.getTimestamp());

        /*
        将事件放入对应窗口
         */
        for (TimeWindow assignedWindow : assignedWindows) {
            windows.computeIfAbsent(event.getKey(), koo -> new HashMap<>()).computeIfAbsent(assignedWindow, koo -> new ArrayList<>()).add(event);
        }
    }

    public void advanceWatermark(long watermarkTimestamp){
        this.currentWatermark = watermarkTimestamp;
        logger.info("watermark advanced to {}", watermarkTimestamp);
    }

    public Map<TimeWindow, List<Event>> getReadyWindows(){
        Map<TimeWindow, List<Event>> readyWindows = new HashMap<>();

        /*
        遍历所有键和窗口
         */
        for (Map.Entry<String, Map<TimeWindow, List<Event>>> keyEntry : windows.entrySet()) {
            for (Map.Entry<TimeWindow, List<Event>> windowEntry : keyEntry.getValue().entrySet()) {
                TimeWindow window = windowEntry.getKey();

                /*
                如果水印超过了窗口结束时间，则窗口就绪
                 */
                if (currentWatermark >= window.getEnd()){
                    readyWindows.putIfAbsent(window, windowEntry.getValue());
                }
            }
        }

        /*
        从活跃窗口中移除已触发的窗口
         */
        readyWindows.keySet().forEach(window -> windows.values().forEach(keyWindows -> keyWindows.remove(window)));

        return readyWindows;
    }
}
