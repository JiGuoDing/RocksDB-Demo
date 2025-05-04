package eventTimeWindow;

import java.util.Arrays;
import java.util.List;

public class FlinkEventTimeWindowSimulator {
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
            if (watermarkIndex)
        }
    }
}
