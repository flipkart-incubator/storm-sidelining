package com.flipkart.message.sidelining.service;

import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.dao.HbaseDataStore;
import com.flipkart.message.sidelining.models.GroupedEvents;
import com.flipkart.message.sidelining.models.Message;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


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

    public boolean sideline(String topic, String groupId, String id, byte[] data){
        log.info("sidelining data {} for topic {} and groupId {}", data, topic, groupId);
        try {
            Message message = new Message();
            message.setGroupId(groupId);
            message.setTopic(topic);
            message.setId(id);
            message.setData(data);
            return hbaseDataStore.insert(message);
        } catch (HBaseClientException e) {
            log.error("error sidelining message {} exception {}", data, e);
            return false;
        }
    }


    public boolean groupSideline(String topic, String groupId, String id, byte[] data) throws Exception {
        log.info("sidelining data in batch for topic {} and groupId {}", topic, groupId);
        try {
            int retryCount = 0;
            while (retryCount <= 10) {
                long version = hbaseDataStore.getVersion(topic, groupId);
                if (version == 0L) {
                    return false;
                } else {
                    Message message = new Message();
                    message.setGroupId(groupId);
                    message.setTopic(topic);
                    message.setId(id);
                    message.setData(data);
                    if (hbaseDataStore.checkAndPut(message, version)) {
                        return true;
                    }
                }
                retryCount++;
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

    //API
    public List<String> getSidelinedRows( String topology, int batch) throws HBaseClientException {
        return hbaseDataStore.getSidelinedRows( topology, batch);
    }

    //API
    public Map<String, String> getInfo(String topic, String groupId, String eventId) throws HBaseClientException {
        log.info("fetching data for topic {}, groupId {} and eventId {}", topic, groupId, eventId);
        Map<String, String> map = new HashMap<>();
        byte[] result = hbaseDataStore.getEvent(topic, groupId, eventId);
        map.put(eventId, Bytes.toString(result));
        map.put("rowKey", HbaseDataStore.getRowKey(topic, groupId));
        return map;
    }

    //API
    public Map<String, String> getInfo(String rowKey, String eventId) throws HBaseClientException {
        log.info("fetching data for rowKey {}, eventId {} ", rowKey, eventId);
        Map<String, String> map = new HashMap<>();
        byte[] result = hbaseDataStore.getEvent(rowKey, eventId);
        map.put(eventId, Bytes.toString(result));
        map.put("rowKey", rowKey);
        return map;
    }

    //API
    public Map<String, String> getInfo(String rowKey) throws HBaseClientException {
        return hbaseDataStore.getInfo(rowKey);
    }

    //API
    public Map getInfo(List<String> topics) throws HBaseClientException {
        Map<String, Object> info = Maps.newHashMap();
        info.put("count", hbaseDataStore.getTotalCount());
        for (String topic : topics) {
            info.put(topic, hbaseDataStore.getTopicCount(topic));
        }
        return info;
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

    public ArrayList<Result> scan(String firstRow, String topology, int batch) throws HBaseClientException {
        return hbaseDataStore.scan(firstRow, topology, batch);
    }

    public List<String> getUnsidelinedRows(String firstRow, String topology, int batch) throws HBaseClientException {
        List<Result> resultList = hbaseDataStore.getUnsidelinedRows(firstRow, topology, batch);
        return resultList.stream().map(result -> Bytes.toString(result.getRow())).collect(Collectors.toList());
    }

    //API
    public void unsideline(String rowKey) {
        try {
            hbaseDataStore.unsideline(rowKey);
        } catch(HBaseClientException e){
            log.error("Error Unsidelining {}", rowKey,e);
        }
    }

    public void finishUnsidelining(String rowKey) {
        try {
            hbaseDataStore.deleteUnsidelineRow(rowKey);
        } catch (HBaseClientException e) {
            log.error("Error deleting rowKey {}", rowKey, e);
        }
    }

    //API
    public boolean update(String topic, String groupId, String id, byte[] data){

        Message message = new Message();
        message.setGroupId(groupId);
        message.setTopic(topic);
        message.setId(id);
        message.setData(data);
        try {
            log.info("updating {}", message);
            hbaseDataStore.update(message);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while updating {} ", message, e);
            return false;
        }
    }


}
