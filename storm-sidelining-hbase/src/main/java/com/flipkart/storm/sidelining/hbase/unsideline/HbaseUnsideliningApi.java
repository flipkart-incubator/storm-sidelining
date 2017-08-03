package com.flipkart.storm.sidelining.hbase.unsideline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.storm.sidelining.core.sideline.models.Message;
import com.flipkart.storm.sidelining.core.unsideline.ModifyFunc;
import com.flipkart.storm.sidelining.core.unsideline.UnsideliningApi;
import com.flipkart.storm.sidelining.core.utils.SerdeUtils;
import com.flipkart.storm.sidelining.hbase.client.HBaseClient;
import com.flipkart.storm.sidelining.hbase.client.HBaseClientException;
import com.flipkart.storm.sidelining.hbase.dao.HbaseHandler;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by gupta.rajat on 27/07/17.
 */
public class HbaseUnsideliningApi implements UnsideliningApi {

    private static final Logger log = LoggerFactory.getLogger(HbaseUnsideliningApi.class);

    private HbaseHandler hbaseHandler;

    public HbaseUnsideliningApi(HTablePool tablePool, String tableName, String unsidelineTable) {
        HBaseClient client = new HBaseClient(tablePool);
        hbaseHandler = new HbaseHandler(client, tableName, unsidelineTable);
    }

    //API
    public void unsidelineAll(int batch) throws HBaseClientException {
        hbaseHandler.unsidelineAll(batch);
    }


    //API
    public void unsidelineTopic(String topic, int batch) throws HBaseClientException {
        hbaseHandler.unsidelineTopic(topic, batch);
    }

    //API
    public void unsidelineGroup(String rowKey) throws HBaseClientException {
        hbaseHandler.unsideline(rowKey);
    }

    //API
    public List<String> getSidelinedRowKeys(int batch) throws HBaseClientException {
        return hbaseHandler.getSidelinedRowKeys(batch);
    }

    //API
    public List<String> getSidelinedRowKeys(String topology, int batch) throws HBaseClientException {
        return hbaseHandler.getSidelinedRowKeys(topology, batch);
    }

    //API
    public Map<String, String> getAllEvents(String rowKey) throws HBaseClientException {
        return hbaseHandler.getAllEvents(rowKey);
    }

    //API
    public Map<String, String> getEvent(String rowKey, String eventId) throws HBaseClientException {
        log.info("fetching data for rowKey {}, eventId {} ", rowKey, eventId);
        Map<String, String> map = new HashMap<>();
        byte[] result = hbaseHandler.getEvent(rowKey, eventId);
        map.put(eventId, Bytes.toString(result));
        map.put("rowKey", rowKey);
        return map;
    }

    //API
    public Map<String, String> getEvent(String topic, String groupId, String eventId) throws HBaseClientException {
        log.info("fetching data for topic {}, groupId {} and eventId {}", topic, groupId, eventId);
        return hbaseHandler.getEvent(topic, groupId, eventId);
    }

    //API
    public boolean update(String topic, String groupId, String id, ModifyFunc modifier) throws HBaseClientException, IOException {
        Map<String, String> allEvents = hbaseHandler.getAllEvents(topic, groupId);
        String event = allEvents.get(id);
        String version = allEvents.get(HbaseHandler.VERSION);
        Map<String, Object> tuple = SerdeUtils.deserialize(event, new TypeReference<Map<String, Object>>() {
        });
        tuple = modifier.modify(tuple);
        byte[] data = SerdeUtils.serialize(tuple);
        Message message = new Message(topic,groupId,id,data);
        return hbaseHandler.checkAndPut(message,Integer.parseInt(version));
    }

    public boolean deleteRow(String topic, String groupId){
        log.info("deleting data for topic {} and groupId {}", topic, groupId);
        try {
            hbaseHandler.deleteRow(topic, groupId);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }
    }

    public boolean deleteEvent(String topic, String groupId, String column) {
        log.info("deleting data for topic {}, groupId {} and column {}", topic, groupId, column);
        try {
            hbaseHandler.deleteColumns(topic, groupId, Lists.newArrayList(column));
            return true;
        } catch (HBaseClientException e) {
            log.error("error while deleting the data {}", e);
            return false;
        }
    }

    public boolean deleteEvent(String rowKey, String column) {
        log.info("deleting data for rowKey {} and column {}", rowKey, column);
        try {
            hbaseHandler.deleteColumn(rowKey, column);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while deleting the data {}", e);
            return false;
        }
    }

    //    //API
    //    public Map getInfo(List<String> topics) throws HBaseClientException {
    //        Map<String, Object> info = Maps.newHashMap();
    //        info.put("count", hbaseHandler.getTotalCount());
    //        for (String topic : topics) {
    //            info.put(topic, hbaseHandler.getTopicCount(topic));
    //        }
    //        return info;
    //    }

    //    //API
    //    public boolean update(String topic, String groupId, String id, byte[] data) {
    //
    //        Message message = new Message(topic, groupId, id, data);
    //        try {
    //            log.info("updating {}", message);
    //            hbaseHandler.update(message);
    //            return true;
    //        } catch (HBaseClientException e) {
    //            log.error("error while updating {} ", message, e);
    //            return false;
    //        }
    //    }
    //

}
