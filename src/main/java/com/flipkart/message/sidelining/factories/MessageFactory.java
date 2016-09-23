package com.flipkart.message.sidelining.factories;

import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.configs.HBaseClientConfig;
import com.flipkart.message.sidelining.service.MessageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * Created by saurabh.jha on 18/09/16.
 */
@Slf4j
public class MessageFactory {

    private static MessageService service;

    public static MessageService getService(HTablePool tablePool) throws HBaseClientException {
        if (tablePool == null)
            throw new HBaseClientException("HTablePool null");
        if (service == null){
            synchronized (MessageFactory.class){
                if (service == null){
                    service = new MessageService(tablePool);
                }
            }
        }
        return service;
    }

    public static MessageService getService() throws HBaseClientException {
        if (service == null)
            throw new HBaseClientException("service not initialised");
        return service;
    }
}
