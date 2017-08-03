package com.flipkart.storm.sidelining.hbase.client;

import com.flipkart.storm.sidelining.core.exceptions.SideliningException;

/**
 * Created by gupta.rajat on 27/07/16.
 */
public class HBaseClientException extends SideliningException {

    public HBaseClientException() {
    }

    public HBaseClientException(String message) {
        super(message);
    }

    public HBaseClientException(Throwable cause) {
        super(cause);
    }

    public HBaseClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseClientException(String message, Throwable cause,
                                boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

