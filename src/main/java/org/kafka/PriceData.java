//To read values from Pricedata JSON Object
package org.kafka;

import java.io.Serializable;

public class PriceData implements Serializable {

    private static final long serialVersionUID = 1L;

    private double open;
    private double high;
    private double low;
    private double close;
    private int volume;

    public Double getOpen() {
        return open;
    }

    public void setOpen(Double open) {
        this.open = open;
    }

    public Double getClose() {
        return close;
    }

    public void setClose(Double close) {
        this.close = close;
    }

    public Double getHigh() {
        return high;
    }

    public void setHigh(Double high) {
        this.high = high;
    }

    public Double getLow() {
        return low;
    }

    public void setLow(Double low) {
        this.low = low;
    }

    public int getVolume() {
        return volume;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }


    @Override
    public String toString() {
        return "priceData {open=" + open + ", close=" + close + ", high=" + high + ", low=" + low +", volume=" + volume +" }";
    }

}


