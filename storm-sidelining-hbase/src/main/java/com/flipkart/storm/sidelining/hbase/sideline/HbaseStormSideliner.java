package com.flipkart.storm.sidelining.hbase.sideline;

import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.storm.sidelining.core.sideline.StormSideliner;
import com.flipkart.storm.sidelining.hbase.client.HBaseClient;
import com.flipkart.storm.sidelining.hbase.client.HBaseClientException;
import com.flipkart.storm.sidelining.hbase.dao.HbaseHandler;
import com.flipkart.storm.sidelining.core.sideline.models.Message;
import com.flipkart.storm.sidelining.core.utils.SerdeUtils;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by gupta.rajat on 05/06/17.
 */

public class HbaseStormSideliner implements StormSideliner{

    private static final Logger log = LoggerFactory.getLogger(HbaseStormSideliner.class);

    private HbaseHandler hbaseHandler;

    public HbaseStormSideliner(HTablePool tablePool, String tableName, String unsidelineTable) {
        HBaseClient client = new HBaseClient(tablePool);
        hbaseHandler = new HbaseHandler(client,tableName, unsidelineTable);
    }

    public boolean sideline(String topic, String groupId, String id, Tuple tuple){
        log.info("sidelining data {} for topic {} and groupId {}", tuple.toString(), topic, groupId);
        try {
            byte[] data = SerdeUtils.serialize(tuple);
            Message message = new Message(topic,groupId,id,data);
            return hbaseHandler.insert(message);
        } catch (HBaseClientException e) {
            log.error("error sidelining message {} ", tuple.toString(), e);
            return false;
        } catch (JsonProcessingException e) {
            log.error("error sidelining message {} ", tuple.toString(), e);
            return false;
        }
    }

    public boolean groupSideline(String topic, String groupId, String id, Tuple tuple) throws Exception {

        try {
            byte[] data = SerdeUtils.serialize(tuple);
            long version = hbaseHandler.getVersion(topic, groupId);
            if (version == 0L) {
                return false;
            } else {
                log.info("sidelining data in batch for topic {}, groupId {} , id {} , data {}", topic, groupId, id, Bytes.toString(data));
                Message message = new Message(topic, groupId, id, data);
                if (hbaseHandler.checkAndPut(message, version)) {
                    return true;
                }
            }
            return false;
        } catch (HBaseClientException e) {
            log.error("error sidelining messages {}", e);
            return false;
        }

    }

}
