package com.flipkart.message.sidelining.factories;

import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.service.SidelineService;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Deprecated //USE DI to get service instead
public class SidelineFactory {
    private static final Logger log = LoggerFactory.getLogger(SidelineFactory.class);

    private static SidelineService service;

    private static boolean initialized = false;

    public static SidelineService getService(HTablePool tablePool, String tableName) {
        if (!initialized) {
            synchronized (SidelineFactory.class) {
                if (!initialized) {
                    service = new SidelineService(tablePool, tableName);
                    initialized = true;
                }
            }
        }
        return service;
    }

    public static SidelineService getService() throws HBaseClientException {
        if (service == null)
            throw new HBaseClientException("service not initialised");
        return service;
    }
}
