package com.flipkart.message.sidelining.service;

import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.dao.HbaseDataStore;
import com.flipkart.message.sidelining.models.GroupedEvents;
import com.flipkart.message.sidelining.models.Message;
import com.flipkart.message.sidelining.utils.SerdeUtils;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.message.sidelining.dao.HbaseDataStore.VERSION;


/**
 * Created by gupta.rajat on 05/06/17.
 */

public class StormSideliner {

    private static final Logger log = LoggerFactory.getLogger(StormSideliner.class);

    private HbaseDataStore hbaseDataStore;

    public StormSideliner(HTablePool tablePool, String tableName, String unsidelineTable) {
        HBaseClient client = new HBaseClient(tablePool);
        hbaseDataStore = new HbaseDataStore(client,tableName, unsidelineTable);
    }

    public boolean sideline(String topic, String groupId, String id, Tuple tuple){
        log.info("sidelining data {} for topic {} and groupId {}", tuple.toString(), topic, groupId);
        try {
            byte[] data = SerdeUtils.serialize(tuple);
            Message message = new Message(topic,groupId,id,data);
            return hbaseDataStore.insert(message);
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
            long version = hbaseDataStore.getVersion(topic, groupId);
            if (version == 0L) {
                return false;
            } else {
                log.info("sidelining data in batch for topic {}, groupId {} , id {} , data {}", topic, groupId, id, Bytes.toString(data));
                Message message = new Message(topic, groupId, id, data);
                if (hbaseDataStore.checkAndPut(message, version)) {
                    return true;
                }
            }
            return false;
        } catch (HBaseClientException e) {
            log.error("error sidelining messages {}", e);
            return false;
        }

    }

    public List<GroupedEvents> getGroupedEvents(List<String> rows) throws HBaseClientException {
        return hbaseDataStore.getGroupedEvents(rows);
    }

    public List<GroupedEvents> getGroupedEvents(String firstRow, String topology, int batch) throws HBaseClientException {
        return hbaseDataStore.getGroupedEvents(firstRow,topology,batch);
    }

    public boolean deleteRow(String topic, String groupId){
        log.info("deleting data for topic {} and groupId {}", topic, groupId);
        try {
            hbaseDataStore.deleteRow(topic, groupId);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }
    }

    public boolean checkAndDeleteRow(String topic, String groupId, long version) {
        log.info("deleting data for topic {} and groupId {}", topic, groupId);
        try {
            hbaseDataStore.checkAndDeleteRow(topic, groupId, version);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }

    }

    public boolean checkAndDeleteRow(String rowKey, long version) {
        log.info("deleting data for rowKey {}", rowKey);
        try {
            hbaseDataStore.checkAndDeleteRow(rowKey, version);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }
    }

    public boolean deleteEvent(String topic, String groupId, String column) {
        log.info("deleting data for topic {}, groupId {} and column {}", topic, groupId, column);
        try {
            hbaseDataStore.deleteColumns(topic, groupId, Lists.newArrayList(column));
            return true;
        } catch (HBaseClientException e) {
            log.error("error while deleting the data {}", e);
            return false;
        }
    }

    public boolean deleteEvent(String rowKey, String column) {
        log.info("deleting data for rowKey {} and column {}", rowKey, column);
        try {
            hbaseDataStore.deleteColumn(rowKey, column);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while deleting the data {}", e);
            return false;
        }
    }


    public List<String> getUnsidelinedRowKeys(String firstRow, String topology, int batch) throws HBaseClientException {
        return hbaseDataStore.getUnsidelinedRowKeys(firstRow, topology, batch);
    }

    public void clearUnsidelinedKeys(List<String> rowKeys) {
        try {
            hbaseDataStore.deleteUnsidelinedRows(rowKeys);
        } catch (HBaseClientException e) {
            log.error("Error deleting rowKeys {}", rowKeys, e);
        }
    }

    //API
    public void unsidelineAll(int batch) throws HBaseClientException {
        hbaseDataStore.unsidelineAll(batch);
    }


    //API
    public void unsidelineTopic(String topic, int batch) throws HBaseClientException {
        hbaseDataStore.unsidelineTopic(topic,batch);
    }

    //API
    public void unsidelineGroup(String rowKey) throws HBaseClientException{
        hbaseDataStore.unsideline(rowKey);
    }

    //API
    public List<String> getSidelinedRowKeys(int batch) throws HBaseClientException {
        return hbaseDataStore.getSidelinedRowKeys(batch);
    }

    //API
    public List<String> getSidelinedRowKeys(String topology, int batch) throws HBaseClientException {
        return hbaseDataStore.getSidelinedRowKeys(topology, batch);
    }

    //API
    public Map<String, String> getAllEvents(String rowKey) throws HBaseClientException {
        return hbaseDataStore.getAllEvents(rowKey);
    }

    //API
    public Map<String, String> getEvent(String rowKey, String eventId) throws HBaseClientException {
        log.info("fetching data for rowKey {}, eventId {} ", rowKey, eventId);
        Map<String, String> map = new HashMap<>();
        byte[] result = hbaseDataStore.getEvent(rowKey, eventId);
        map.put(eventId, Bytes.toString(result));
        map.put("rowKey", rowKey);
        return map;
    }

    //API
    public Map<String, String> getEvent(String topic, String groupId, String eventId) throws HBaseClientException {
        log.info("fetching data for topic {}, groupId {} and eventId {}", topic, groupId, eventId);
        Map<String, String> map = new HashMap<>();
        byte[] result = hbaseDataStore.getEvent(topic, groupId, eventId);
        map.put(eventId, Bytes.toString(result));
        map.put("rowKey", HbaseDataStore.getRowKey(topic, groupId));
        return map;
    }

//    //API
//    public Map getInfo(List<String> topics) throws HBaseClientException {
//        Map<String, Object> info = Maps.newHashMap();
//        info.put("count", hbaseDataStore.getTotalCount());
//        for (String topic : topics) {
//            info.put(topic, hbaseDataStore.getTopicCount(topic));
//        }
//        return info;
//    }

    //API
    public boolean update(String topic, String groupId, String id, byte[] data){

        Message message = new Message(topic,groupId,id,data);
        try {
            log.info("updating {}", message);
            hbaseDataStore.update(message);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while updating {} ", message, e);
            return false;
        }
    }


    public boolean update(String topic, String groupId, String id, ModifyFunc modifier) throws HBaseClientException, IOException {
        Map<String, String> allEvents = hbaseDataStore.getAllEvents(topic, groupId);
        String event = allEvents.get(id);
        String version = allEvents.get(VERSION);
        Map<String, Object> tuple = SerdeUtils.deserialize(event, new TypeReference<Map<String, Object>>() {
        });
        tuple = modifier.modify(tuple);
        byte[] data = SerdeUtils.serialize(tuple);
        Message message = new Message(topic,groupId,id,data);
        return hbaseDataStore.checkAndPut(message,Integer.parseInt(version));
    }

}
