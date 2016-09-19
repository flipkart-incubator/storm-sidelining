package com.flipkart.message.sidelining.factories;

import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.service.MessageService;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by saurabh.jha on 18/09/16.
 */
public class MessageFactory {

    private static MessageService service;

    public static MessageService getService(Configuration config, int poolSize) throws HBaseClientException {
        if (service == null){
            synchronized (MessageFactory.class){
                if (service == null){
                    service = new MessageService(config, poolSize);
                }
            }
        }
        return service;
    }
}
