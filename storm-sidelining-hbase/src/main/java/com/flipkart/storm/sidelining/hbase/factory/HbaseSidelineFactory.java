package com.flipkart.storm.sidelining.hbase.factory;

import com.flipkart.storm.sidelining.hbase.client.HBaseClientException;
import com.flipkart.storm.sidelining.hbase.sideline.HbaseStormSideliner;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HbaseSidelineFactory {
    private static final Logger log = LoggerFactory.getLogger(HbaseSidelineFactory.class);

    private static HbaseStormSideliner service;

    private static boolean initialized = false;

    public static HbaseStormSideliner getService(HTablePool tablePool, String tableName, String unsidelineTable) {
        if (!initialized) {
            synchronized (HbaseSidelineFactory.class) {
                if (!initialized) {
                    service = new HbaseStormSideliner(tablePool, tableName, unsidelineTable);
                    initialized = true;
                }
            }
        }
        return service;
    }

    public static HbaseStormSideliner getService() throws HBaseClientException {
        if (service == null)
            throw new HBaseClientException("service not initialised");
        return service;
    }
}
