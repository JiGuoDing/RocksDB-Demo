package eventTimeWindow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FlinkEventTimeWindowSimulator {
    private static final Logger logger = LoggerFactory.getLogger(FlinkEventTimeWindowSimulator.class);
    private static final Logger log = LoggerFactory.getLogger(FlinkEventTimeWindowSimulator.class);

    public static void main(String[] args) {
        /*
        创建窗口管理器，窗口大小为 5s
         */
        WindowManager windowManager = new WindowManager(5000);

        /*
        模拟事件流
         */
        List<Event> events = Arrays.asList(
                new Event("key1", 1000, "event1"),  // 窗口[0-5000]
                new Event("key1", 2000, "event2"),  // 窗口[0-5000]
                new Event("key1", 6000, "event3"),  // 窗口[5000-10000]
                new Event("key1", 4000, "event4"),  // 窗口[0-5000] (乱序事件)
                new Event("key2", 3000, "event5"),  // 窗口[0-5000]
                new Event("key1", 12000, "event6")  // 窗口[10000-15000]
        );

        /*
        模拟 watermark 生成
         */
        List<Watermark> watermarks = Arrays.asList(
                new Watermark(3000),
                new Watermark(6000),
                new Watermark(15000)
        );

        /*
        处理 event 和 watermark
         */
        int eventIndex = 0;
        int watermarkIndex = 0;

        while (eventIndex < events.size() || watermarkIndex < watermarks.size()){
            /*
            优先处理 watermark
             */
            if (watermarkIndex < watermarks.size() && (eventIndex >= events.size() || watermarks .get(watermarkIndex).getTimestamp() <= events.get(eventIndex).getTimestamp())){
                Watermark watermark = watermarks.get(watermarkIndex++);
                windowManager.advanceWatermark(watermark.getTimestamp());
                
                /*
                检查是否有窗口可以触发
                 */
                Map<TimeWindow, List<Event>> readyWindows = windowManager.getReadyWindows();

                if (!readyWindows.isEmpty()){
                    logger.info("Triggered Windows at watermark {}", watermark.getTimestamp());
                    readyWindows.forEach((window, windowEvents) -> {
                        logger.info("Window {} has events: ", window);
                        windowEvents.forEach(event -> {
                            logger.info("{} @ {} : {}", event.getKey(), event.getTimestamp(), event.getValue());
                            /*
                            TODO 此处添加窗口计算逻辑
                             */
                        });
                    });
                }
            } else if (eventIndex < events.size()){
                Event event = events.get(eventIndex++);
                logger.info("Processing event: {} @ {} : {}", event.getKey(), event.getTimestamp(), event.getValue());
                windowManager.processEvent(event);
            }
        }
    }
}
