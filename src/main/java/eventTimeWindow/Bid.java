package eventTimeWindow;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** A bid for an item on auction. */
public class Bid implements Serializable {

    /** Id of auction this bid is for. */
    public long auction; // foreign key: Auction.id

    /** Id of person bidding in auction. */
    public long bidder; // foreign key: Person.id

    /** Price of bid, in cents. */
    public long price;

    /** The channel introduced this bidding. */
    public String channel;

    /** The url of this bid. */
    public String url;

    /**
     * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
     * event time.
     */
    public Instant dateTime;

    /** Additional arbitrary payload for performance testing. */
    public String extra;

    public Bid(long auction, long bidder, long price, String channel, String url, Instant dateTime, String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.channel = channel;
        this.url = url;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        Bid other = (Bid) otherObject;
        return Objects.equals(auction, other.auction)
                && Objects.equals(bidder, other.bidder)
                && Objects.equals(price, other.price)
                && Objects.equals(channel, other.channel)
                && Objects.equals(url, other.url)
                && Objects.equals(dateTime, other.dateTime)
                && Objects.equals(extra, other.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hash(auction, bidder, price, channel, url, dateTime, extra);
    }

    @Override
    public String toString() {
        return "Bid{" +
                "auction=" + auction +
                ", bidder=" + bidder +
                ", price=" + price +
                ", channel=" + channel +
                ", url=" + url +
                ", dateTime=" + dateTime +
                ", extra='" + extra + '\'' +
                '}';
    }
}