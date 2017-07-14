package com.flipkart.message.sidelining.configs;

import java.io.Serializable;

/**
 * Created by gupta.rajat on 04/07/17.
 */
public class UnSideliningConfig implements Serializable{
    public int batch = 10;
    public int partitions = 512;

    public Mode mode = Mode.SEMI_AUTOMATIC;

    public enum Mode {
        AUTOMATIC,
        SEMI_AUTOMATIC,
        MANUAL
    }
}


