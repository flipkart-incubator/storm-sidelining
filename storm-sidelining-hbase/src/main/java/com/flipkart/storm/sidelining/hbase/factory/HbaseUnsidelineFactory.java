package com.flipkart.storm.sidelining.hbase.factory;

/**
 * Created by gupta.rajat on 28/07/17.
 */

import com.flipkart.storm.sidelining.hbase.client.HBaseClientException;
import com.flipkart.storm.sidelining.hbase.unsideline.HbaseStormUnsideliner;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HbaseUnsidelineFactory {
    private static final Logger log = LoggerFactory.getLogger(HbaseUnsidelineFactory.class);

    private static HbaseStormUnsideliner service;

    private static boolean initialized = false;

    public static HbaseStormUnsideliner getService(HTablePool tablePool, String tableName, String unsidelineTable) {
        if (!initialized) {
            synchronized (HbaseUnsidelineFactory.class) {
                if (!initialized) {
                    service = new HbaseStormUnsideliner(tablePool, tableName, unsidelineTable);
                    initialized = true;
                }
            }
        }
        return service;
    }

    public static HbaseStormUnsideliner getService() throws HBaseClientException {
        if (service == null)
            throw new HBaseClientException("service not initialised");
        return service;
    }
}

