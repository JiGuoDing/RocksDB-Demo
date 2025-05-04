package eventTimeWindow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WindowManager {
    private Map<String, Map<TimeWindow, List<Event>>> windows = new HashMap<>();
    private WindowAssigner windowAssigner;
    private long currentWatermark = Long.MIN_VALUE;
    private static final Logger logger = LoggerFactory.getLogger(WindowManager.class);

    public WindowManager(long windowSize){
        this.windowAssigner = new WindowAssigner(windowSize);
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
            windows.computeIfAbsent(event.getKey(), k -> new HashMap<>()).computeIfAbsent(assignedWindow, k -> new ArrayList<>()).add(event);
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
        readyWindows.keySet().forEach(window -> {
            windows.values().forEach(keyWindows -> keyWindows.remove(window));
        });

        return readyWindows;
    }
}
