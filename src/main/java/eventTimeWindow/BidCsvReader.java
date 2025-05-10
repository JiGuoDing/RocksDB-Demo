package eventTimeWindow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class BidCsvReader {

    private static final Logger LOG = LoggerFactory.getLogger(BidCsvReader.class);

    private static final DateTimeFormatter[] DATE_TIME_FORMATTERS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),  // 完整毫秒
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"),   // 2位毫秒
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"),    // 1位毫秒
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")       // 无毫秒
    };

    public static List<Bid> readBidsFromDirectory1(String directoryPath) {
        List<Bid> bids = new ArrayList<>();

        try {
            // 1. 获取目录下所有CSV文件
            File dir = new File(directoryPath);
            File[] files = dir.listFiles((d, name) -> name.endsWith(".csv"));

            if (files == null || files.length == 0) {
                System.out.println("No CSV files found in directory: " + directoryPath);
                return bids;
            }

            /*
            按文件名排序
             */
            Arrays.sort(files, Comparator.comparing(File::getName));

            // 2. 计算前80%的文件数量
            int totalFileNum = files.length;
            int filesToRead = (int) Math.ceil(totalFileNum * 0.6);
            System.out.println("Reading " + filesToRead + " out of " + totalFileNum + " files");

            // 3. 读取前80%的文件
            for (int i = 0; i < filesToRead; i++) {
                LOG.info("Reading {}th file {}", i, files[i].getName());
                try (BufferedReader br = new BufferedReader(new FileReader(files[i]))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        Bid bid = parseBid(line);
                        if (bid != null) {
                            bids.add(bid);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Read {} bids", bids.size());

        return bids;
    }

    public static List<Bid> readBidsFromDirectory2(String directoryPath) {
        List<Bid> bids = new ArrayList<>();

        try {
            // 1. 获取目录下所有CSV文件
            File dir = new File(directoryPath);
            File[] files = dir.listFiles((d, name) -> name.endsWith(".csv"));

            if (files == null || files.length == 0) {
                System.out.println("No CSV files found in directory: " + directoryPath);
                return bids;
            }
            /*
            按文件名排序
             */
            Arrays.sort(files, Comparator.comparing(File::getName));

            // 2. 计算后20%的文件数量
            int totalFileNum = files.length;
            int filesToSkip = (int) Math.floor(totalFileNum * 0.8); // 跳过前80%
            int filesToRead = totalFileNum - filesToSkip; // 读取后20%

            System.out.println("Reading last " + filesToRead + " out of " + totalFileNum + " files");

            // 3. 读取后20%的文件
            for (int i = filesToSkip; i < totalFileNum; i++) {
                LOG.info("Reading {}th file {}", i, files[i].getName());
                try (BufferedReader br = new BufferedReader(new FileReader(files[i]))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        Bid bid = parseBid(line);
                        if (bid != null) {
                            bids.add(bid);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOG.info("Read {} bids", bids.size());

        return bids;
    }

    private static Bid parseBid(String csvLine) {
        try {
            // 使用正则表达式分割CSV，避免分割引号内的逗号
            String[] values = csvLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (values.length < 7) {
                System.err.println("Invalid CSV line: " + csvLine);
                return null;
            }

            // 去除所有字段的双引号（如果存在）
            long auction = Long.parseLong(values[0].replace("\"", "").trim());
            long bidder = Long.parseLong(values[1].replace("\"", "").trim());
            long price = Long.parseLong(values[2].replace("\"", "").trim());
            String channel = values[3].replace("\"", "").trim();
            String url = values[4].replace("\"", "").trim();

            // 处理时间戳字段（特殊处理）
            String dateTimeStr = values[5].replace("\"", "").trim();
            Instant dateTime = parseFlexibleDateTime(dateTimeStr);

            String extra = values[6].replace("\"", "").trim();

            return new Bid(auction, bidder, price, channel, url, dateTime, extra);
        } catch (Exception e) {
            System.err.println("Error parsing line: " + csvLine);
            e.printStackTrace();
            return null;
        }
    }

    private static Instant parseFlexibleDateTime(String dateTimeStr) {
        for (DateTimeFormatter formatter : DATE_TIME_FORMATTERS) {
            try {
                return LocalDateTime.parse(dateTimeStr, formatter)
                        .atZone(ZoneId.systemDefault())
                        .toInstant();
            } catch (DateTimeParseException ignored) {}
        }
        throw new DateTimeParseException("Failed to parse date time: " + dateTimeStr, dateTimeStr, 0);
    }

}
