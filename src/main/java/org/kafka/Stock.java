//Class for JSON Message
package org.kafka;

import java.io.Serializable;


public class Stock implements Serializable {

    private static final long serialVersionUID = 1L;
    private String symbol;
    private String timestamp;
    private PriceData priceData;

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }


    public String getTimestamp() {
        return timestamp;
    }

    public void setPricedata(PriceData priceData) {
        this.priceData = priceData;
    }

    public PriceData getPriceData() {
        return priceData;
    }

    @Override
    public String toString() {
        return "Stock [symbol=" + symbol + ", timestamp=" + timestamp + priceData.toString() + " ]";
    }

}






