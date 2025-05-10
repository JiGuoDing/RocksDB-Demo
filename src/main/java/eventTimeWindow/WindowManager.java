package eventTimeWindow;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class WindowManager {
    private final Map<String, Map<TimeWindow, List<Event>>> windows = new HashMap<>();
    private final WindowAssigner windowAssigner;
    private long currentWatermark = Long.MIN_VALUE;
    private static final Logger logger = LoggerFactory.getLogger(WindowManager.class);
    private StateBackend stateBackend;
    private final Random random = new Random();

    public WindowManager(long windowSize) throws RocksDBException, IOException {
        this.windowAssigner = new WindowAssigner(windowSize);
        this.stateBackend = new StateBackend("data/rocksdb");
    }

    /*
    初始化测试状态数据
     */
    public List<Bid> initTestData() throws RocksDBException, IOException {
        // for (int i = 0; i < 100000000; i++) {
        //     Bid bid = generateTestBid(i);
        //     if (i % 100000 == 0)
        //         logger.info("Initializing test bid for auction {}", bid.auction);
        //     stateBackend.putState(String.valueOf(bid.auction), BidSerializer.serialize(bid));
        // }
        List<Bid> bids = BidCsvReader.readBidsFromDirectory1("/data1/jgd/data/q20_input");
        int bidsLength = bids.size();
        for (int i = 0; i < bidsLength; i++) {
            Bid bid = bids.get(i);
            if (i % 100000 == 0)
                logger.info("Initializing test bid ({}/{})", i, bidsLength);
            try {
                byte[] bidBytes = BidSerializer.serialize(bid);
                if (stateBackend.exists(bidBytes)){
                    byte[] state = stateBackend.getState(bidBytes);
                    long num = Long.parseLong(Arrays.toString(state));
                    num++;
                    stateBackend.putState(bidBytes, String.valueOf(num).getBytes());
                } else
                    stateBackend.putState(bidBytes, "1".getBytes());
            } catch (IOException | RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        return bids;
    }

    private Bid generateTestBid(long auctionId) {
        String largeExtra = generateLargeString(1024);
        return new Bid(
                auctionId,
                new SplittableRandom().nextLong(10000), // random bidder
                new SplittableRandom().nextLong(1000),  // random price (cents)
                "channel-" + auctionId % 10,
                "http://example.com/auction/" + auctionId,
                Instant.now(),
                "test-extra-" + largeExtra + auctionId
        );
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

    // public void prefetchForEvent(Event event) throws RocksDBException {
    //     /*
    //     预测下一个窗口可能访问的key
    //      */
    //     List<String> keysToPrefetch = Arrays.asList(
    //             "key_" + (event.getTimestamp() % 10000),
    //             "key_" + ((event.getTimestamp() + this.windowAssigner.getWindowSize()) % 10000)
    //     );
    //     stateBackend.prefetch(keysToPrefetch);
    // }

    /*
    为指定的 n 个 event 预取状态
     */
    public void prefetchStateForEvent(List<Event> events) {
        List<byte[]> keysToPrefetch = new ArrayList<>();

        events.forEach(event -> {
            try {
                keysToPrefetch.add(BidSerializer.serialize(event.getKey()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
            windows.computeIfAbsent(event.getKey().toString(), koo -> new HashMap<>()).computeIfAbsent(assignedWindow, koo -> new ArrayList<>()).add(event);
        }

        /*
        模拟对事件进行一定操作的耗时
         */
        // try {
        //     int sleepTime = 10 + random.nextInt(41);
        //     logger.info("processing event {} within {}ms", event, sleepTime);
        //     Thread.sleep(sleepTime);
        // } catch (InterruptedException e) {
        //     throw new RuntimeException(e);
        // }
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

    private String generateLargeString(int targetSizeBytes) {
        // 每个字符占 2 bytes（UTF-16），所以字符数 = targetSizeBytes / 2
        int charCount = targetSizeBytes / 2;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < charCount; i++) {
            sb.append('x'); // 填充任意字符
        }
        return sb.toString();
    }
}
