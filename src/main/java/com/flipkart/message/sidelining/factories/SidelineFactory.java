package com.flipkart.message.sidelining.factories;

import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.service.StormSideliner;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Deprecated //USE DI to get service instead
public class SidelineFactory {
    private static final Logger log = LoggerFactory.getLogger(SidelineFactory.class);

    private static StormSideliner service;

    private static boolean initialized = false;

    public static StormSideliner getService(HTablePool tablePool, String tableName, String unsidelineTable) {
        if (!initialized) {
            synchronized (SidelineFactory.class) {
                if (!initialized) {
                    service = new StormSideliner(tablePool, tableName, unsidelineTable);
                    initialized = true;
                }
            }
        }
        return service;
    }

    public static StormSideliner getService() throws HBaseClientException {
        if (service == null)
            throw new HBaseClientException("service not initialised");
        return service;
    }
}
