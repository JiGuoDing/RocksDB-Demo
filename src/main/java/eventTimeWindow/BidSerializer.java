package eventTimeWindow;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;

public class BidSerializer {

    private static RowDataSerializer serializer = new RowDataSerializer(BidRowDataConverter.ROW_TYPE);
    private static final Logger LOG = LoggerFactory.getLogger(BidSerializer.class);
    public static byte[] serialize(Bid bid) throws IOException {

        RowData rowData = BidRowDataConverter.bidToRowData(bid);


        // 初始化DataOutputView
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputView outputView = new DataOutputViewStreamWrapper(baos);
        serializer.serialize(rowData, outputView);
        return baos.toByteArray();

        // try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        //      ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        //     oos.writeObject(bid);
        //     return bos.toByteArray();
        // } catch (IOException e) {
        //     throw new RuntimeException(e);
        // }
    }

    // 反序列化 byte[] -> Bid
    public static Bid deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Bid) ois.readObject();
        }
    }

    public static void main(String[] args) {
        Bid bid = new Bid(1000,2001,14332305,"Apple","https://www.nexmark.com/wmvb/vzg/glp/item.htm?query=1",Instant.now(),"__WQPPTUuetsboKQL^[\\czbkprOSWTaLYVXVNKWMWTIXoityygURXQUNgvqbkwo\"");
        try {
            byte[] serializedBid = serialize(bid);
            LOG.info("serializedBid size: {}", serializedBid.length);
            StateBackend backend = new StateBackend("tmp/rocksDB");
            backend.putState(serializedBid, "49".getBytes());
            byte[] state = backend.getState(serializedBid);
            long num = Long.parseLong(new String(state));
            backend.putState(serializedBid, String.valueOf(num + 1).getBytes());
            state = backend.getState(serializedBid);
            LOG.info("state: {}", new String(state));
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
        }
    }
}
