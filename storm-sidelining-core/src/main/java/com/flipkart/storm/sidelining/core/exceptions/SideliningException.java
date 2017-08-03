package com.flipkart.storm.sidelining.core.exceptions;

/**
 * Created by gupta.rajat on 27/07/17.
 */
public class SideliningException extends Exception {
    public SideliningException() {
    }
    public SideliningException(String message) {
        super(message);
    }

    public SideliningException(String message, Throwable cause) {
        super(message, cause);
    }

    public SideliningException(Throwable cause) {
        super(cause);
    }

    public SideliningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
