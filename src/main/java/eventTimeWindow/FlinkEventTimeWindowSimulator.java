package eventTimeWindow;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FlinkEventTimeWindowSimulator {
    private static final Logger logger = LoggerFactory.getLogger(FlinkEventTimeWindowSimulator.class);
    private static final AtomicInteger totalEvents = new AtomicInteger();
    private static final AtomicLong totalProcessingTime = new AtomicLong();

    private static void testGet() throws RocksDBException, InterruptedException {
        WindowManager windowManager = new WindowManager(5000);

        // 生成测试数据
        List<Event> events = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < 5000; i++) {
            long timestamp = rand.nextInt(200000);
            String key = "key_" + rand.nextInt(2000);
            events.add(new Event(key, timestamp, "event_" + i));
        }
        /*
        按时间戳升序排列事件
         */
        events.sort(Comparator.comparingLong(Event::getTimestamp));

        // 水印生成（每5000ms一个）
        List<Watermark> watermarks = new ArrayList<>();
        for (long t = 0; t <= 200000; t += 5000) {
            watermarks.add(new Watermark(t));
        }

        // 处理循环（添加计时）
        int eventIndex = 0;
        int watermarkIndex = 0;
        long startTime = System.currentTimeMillis();

        while (eventIndex < events.size() || watermarkIndex < watermarks.size()) {
            /*
            条件1：水印尚未达到上限
            条件2：
                条件2.1：所有事件全部到达
                条件2.2：下一个水印的时间戳不大于下一个事件的时间戳
             */
            if (watermarkIndex < watermarks.size() &&
                    (eventIndex >= events.size() ||
                            watermarks.get(watermarkIndex).getTimestamp() <= events.get(eventIndex).getTimestamp())) {

                // 处理水印
                Watermark watermark = watermarks.get(watermarkIndex++);
                windowManager.advanceWatermark(watermark.getTimestamp());

                Map<TimeWindow, List<Event>> readyWindows = windowManager.getReadyWindows();
                if (!readyWindows.isEmpty()) {
                    readyWindows.forEach((window, windowEvents) -> {
                        long windowStart = System.currentTimeMillis();
                        windowEvents.forEach(event -> {
                            logger.info("Triggered Window {}", window);
                        });
                        totalProcessingTime.addAndGet(System.currentTimeMillis() - windowStart);
                    });
                }
            } else {
                Event event = events.get(eventIndex++);

                logger.info("Receiving event: {}", event);
                long eventStart = System.currentTimeMillis();
                /*
                将事件放入对应窗口，并执行一定操作
                 */
                windowManager.processEvent(event);

                /*
                读取状态
                 */
                logger.info("Getting event {}'s status", event);
                byte[] state = windowManager.getStateBackend().getState(event.getKey());
                windowManager.getStateBackend().putState(event.getKey(), state);
                logger.info("Finishing getting event {}'s status", event);
                logger.info("Block Cache Hit Ratio: {}", windowManager.getStateBackend().getCacheHitRate());

                totalProcessingTime.addAndGet(System.currentTimeMillis() - eventStart);
                totalEvents.incrementAndGet();

                /*
                对后续100个事件进行状态异步预取
                 */
                int endIndex = Math.min(eventIndex + 100, events.size());
                if (eventIndex < events.size()) {
                    List<Event> nextEvents = events.subList(eventIndex, endIndex);
                    windowManager.getStateBackend().clearPrefetch();
                    windowManager.prefetchStateForEvent(nextEvents);
                }
            }
        }

        windowManager.getStateBackend().close();

        // 输出性能统计
        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Total events processed: {}", totalEvents.get());
        logger.info("Total processing time: {}ms", totalTime);
        logger.info("Average latency: {}ms/event",
                (double)totalProcessingTime.get() / totalEvents.get());
    }

    private static void testMultiGet() throws RocksDBException, InterruptedException {
        WindowManager windowManager = new WindowManager(5000);

        // 生成测试数据
        List<Event> events = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < 5000; i++) {
            long timestamp = rand.nextInt(200000);
            String key = "key_" + rand.nextInt(2000);
            events.add(new Event(key, timestamp, "event_" + i));
        }
        /*
        按时间戳升序排列事件
         */
        events.sort(Comparator.comparingLong(Event::getTimestamp));

        /*
        批次事件
         */
        List<byte[]> batchEventKeys = new ArrayList<>();

        // 水印生成（每5000ms一个）
        List<Watermark> watermarks = new ArrayList<>();
        for (long t = 0; t <= 200000; t += 5000) {
            watermarks.add(new Watermark(t));
        }

        // 处理循环（添加计时）
        int eventIndex = 0;
        int watermarkIndex = 0;
        long startTime = System.currentTimeMillis();

        while (eventIndex < events.size() || watermarkIndex < watermarks.size()) {
            /*
            条件1：水印尚未达到上限
            条件2：
                条件2.1：所有事件全部到达
                条件2.2：下一个水印的时间戳不大于下一个事件的时间戳
             */
            if (watermarkIndex < watermarks.size() &&
                    (eventIndex >= events.size() ||
                            watermarks.get(watermarkIndex).getTimestamp() <= events.get(eventIndex).getTimestamp())) {

                // 处理水印
                Watermark watermark = watermarks.get(watermarkIndex++);
                windowManager.advanceWatermark(watermark.getTimestamp());

                Map<TimeWindow, List<Event>> readyWindows = windowManager.getReadyWindows();
                if (!readyWindows.isEmpty()) {
                    readyWindows.forEach((window, windowEvents) -> {
                        long windowStart = System.currentTimeMillis();
                        windowEvents.forEach(event -> {
                            logger.info("Triggered Window {}", window);
                        });
                        totalProcessingTime.addAndGet(System.currentTimeMillis() - windowStart);
                    });
                }
            } else {
                Event event = events.get(eventIndex++);

                logger.info("Receiving event: {}", event);
                /*
                将事件放入对应窗口，并执行一定操作
                 */
                windowManager.processEvent(event);

                /*
                攒批次100个key再读取状态
                 */
                batchEventKeys.add(event.getKey().getBytes());
                if (batchEventKeys.size() >= 100){
                    logger.info("Collected 100 events, start to fetch status");
                    long eventStart = System.currentTimeMillis();

                    List<byte[]> stateList = windowManager.getStateBackend().multiGet(batchEventKeys);
                    totalProcessingTime.addAndGet(System.currentTimeMillis() - eventStart);
                    logger.info("Block Cache Hit Ratio: {}", windowManager.getStateBackend().getCacheHitRate());

                    windowManager.getStateBackend().multiPut(batchEventKeys, stateList);
                    batchEventKeys.clear();

                    /*
                    对后续100个事件进行状态异步预取
                     */
                    int endIndex = Math.min(eventIndex + 100, events.size());
                    if (eventIndex < events.size()) {
                        List<Event> nextEvents = events.subList(eventIndex, endIndex);
                        windowManager.getStateBackend().clearPrefetch();
                        windowManager.prefetchStateForEvent(nextEvents);
                    }
                }

                totalEvents.incrementAndGet();
            }
        }

        windowManager.getStateBackend().close();

        // 输出性能统计
        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Total events processed: {}", totalEvents.get());
        logger.info("Total processing time: {}ms", totalTime);
        logger.info("Average latency: {}ms/event",
                (double)totalProcessingTime.get() / totalEvents.get());
    }

    public static void main(String[] args) throws RocksDBException, InterruptedException {
        testGet();
        // testMultiGet();













        // /*
        // 创建窗口管理器，窗口大小为 5s
        //  */
        // WindowManager windowManager = new WindowManager(5000);
        //
        // /*
        // 模拟事件流
        //  */
        // List<Event> events = Arrays.asList(
        //         new Event("key1", 1000, "event1"),  // 窗口[0-5000]
        //         new Event("key1", 2000, "event2"),  // 窗口[0-5000]
        //         new Event("key1", 6000, "event3"),  // 窗口[5000-10000]
        //         new Event("key1", 4000, "event4"),  // 窗口[0-5000] (乱序事件)
        //         new Event("key2", 3000, "event5"),  // 窗口[0-5000]
        //         new Event("key1", 12000, "event6")  // 窗口[10000-15000]
        // );
        //
        // /*
        // 模拟 watermark 生成
        //  */
        // List<Watermark> watermarks = Arrays.asList(
        //         new Watermark(3000),
        //         new Watermark(6000),
        //         new Watermark(15000)
        // );
        //
        // /*
        // 处理 event 和 watermark
        //  */
        // int eventIndex = 0;
        // int watermarkIndex = 0;
        //
        // while (eventIndex < events.size() || watermarkIndex < watermarks.size()){
        //     /*
        //     优先处理 watermark
        //      */
        //     if (watermarkIndex < watermarks.size() && (eventIndex >= events.size() || watermarks .get(watermarkIndex).getTimestamp() <= events.get(eventIndex).getTimestamp())){
        //         Watermark watermark = watermarks.get(watermarkIndex++);
        //         windowManager.advanceWatermark(watermark.getTimestamp());
        //
        //         /*
        //         检查是否有窗口可以触发
        //          */
        //         Map<TimeWindow, List<Event>> readyWindows = windowManager.getReadyWindows();
        //
        //         if (!readyWindows.isEmpty()){
        //             logger.info("Triggered Windows at watermark {}", watermark.getTimestamp());
        //             readyWindows.forEach((window, windowEvents) -> {
        //                 logger.info("Window {} has event: ", window);
        //                 windowEvents.forEach(event -> {
        //                     /*
        //                     输出该窗口的所有事件
        //                      */
        //                     logger.info("{} @ {} : {}", event.getKey(), event.getTimestamp(), event.getValue());
        //                     /*
        //                     TODO 此处添加窗口计算逻辑
        //                      */
        //                 });
        //             });
        //         }
        //     } else if (eventIndex < events.size()){
        //         /*
        //         当不满足水印处理条件时，处理下一个事件
        //          */
        //         Event event = events.get(eventIndex++);
        //         logger.info("Processing event: {} @ {} : {}", event.getKey(), event.getTimestamp(), event.getValue());
        //         windowManager.processEvent(event);
        //     }
        // }
    }
}
