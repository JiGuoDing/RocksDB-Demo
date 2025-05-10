package eventTimeWindow;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.Instant;
import java.util.Arrays;

public class BidRowDataConverter {

    // 精确计算后的字段长度
    public static final RowType ROW_TYPE = new RowType(Arrays.asList(
            new RowType.RowField("auction", new BigIntType()),
            new RowType.RowField("bidder", new BigIntType()),
            new RowType.RowField("price", new BigIntType()),
            new RowType.RowField("channel", new VarCharType(20)),
            new RowType.RowField("url", new VarCharType(100)),
            new RowType.RowField("dateTime", new TimestampType()),
            new RowType.RowField("extra", new VarCharType(200))
    ));
    public static RowData bidToRowData(Bid bid) {
        GenericRowData rowData = new GenericRowData(7);
        rowData.setField(0, bid.auction);
        rowData.setField(1, bid.bidder);
        rowData.setField(2, bid.price);
        rowData.setField(3, StringData.fromString(bid.channel));
        rowData.setField(4, StringData.fromString(bid.url));
        rowData.setField(5, TimestampData.fromInstant(bid.dateTime));
        rowData.setField(6, StringData.fromString(bid.extra));
        return rowData;
    }

    public static Bid rowDataToBid(RowData rowData) {
        return new Bid(
                rowData.getLong(0),
                rowData.getLong(1),
                rowData.getLong(2),
                rowData.getString(3).toString(),
                rowData.getString(4).toString(),
                Instant.ofEpochMilli(rowData.getLong(5)),
                rowData.getString(6).toString()
        );
    }

    
}
