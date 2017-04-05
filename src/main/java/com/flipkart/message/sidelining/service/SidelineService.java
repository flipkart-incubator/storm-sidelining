package com.flipkart.message.sidelining.service;

import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.dao.HBaseDAO;
import com.flipkart.message.sidelining.models.Message;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static com.flipkart.message.sidelining.configs.HBaseTableConfig.CF;

/**
 * Created by saurabh.jha on 16/09/16.
 */

//TODO atomic version updates
public class SidelineService {

    private static final Logger log = LoggerFactory.getLogger(SidelineService.class);

    private HBaseDAO hBaseDAO;

    public SidelineService(HTablePool tablePool, String tableName) {
        HBaseClient client = new HBaseClient(tablePool);
        hBaseDAO = new HBaseDAO(client,tableName);
    }

    public boolean sideline(String topic, String groupId, String id, byte[] data){
        log.info("sidelining data {} for topic {} and groupId {}", data, topic, groupId);
        try {
            Message message = new Message();
            message.setGroupId(groupId);
            message.setTopic(topic);
            message.setId(id);
            message.setData(data);
            hBaseDAO.insert(message);
        } catch (HBaseClientException e) {
            log.error("error sidelining message {} exception {}", data, e);
            return false;
        }
        return true;
    }


    public boolean groupSideline(String topic, String groupId, String id, byte[] data) throws Exception {
        log.info("sidelining data in batch for topic {} and groupId {}", topic, groupId);
        try {
            int retryCount = 0;
            while (retryCount <= 10) {
                long version = hBaseDAO.getVersion(topic, groupId);
                if (version == 0L) {
                    return false;
                } else {
                    Message message = new Message();
                    message.setGroupId(groupId);
                    message.setTopic(topic);
                    message.setId(id);
                    message.setData(data);
                    if (hBaseDAO.checkAndPut(message, version)) {
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

    public Map<String, byte[]> fetchData(String topic, String groupId) throws HBaseClientException {
        log.info("fetching data for topic {} and groupId {}", topic, groupId);
        Map<String, byte[]> map = new HashMap<>();
        Result result = hBaseDAO.get(topic, groupId);
        NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(CF));
        if (navigableMap == null || navigableMap.size() == 0)
            return map;
        for (byte[] bytes : navigableMap.keySet()){
            String key = Bytes.toString(bytes);
            map.put(key, navigableMap.get(bytes));
        }
        return map;
    }

//    @Deprecated // make it thread safe
//    public boolean validateAndUpdate(String topic, String groupId, String id, byte[] data) {
//        log.info("validaing data {} for topic {} and groupId {}", data, topic, groupId);
//        try {
//            Result result = hBaseDAO.get(topic, groupId);
//            NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(CF));
//            if (navigableMap != null && navigableMap.size() > 0){
//                Message message = new Message();
//                message.setId(id);
//                message.setTopic(topic);
//                message.setGroupId(groupId);
//                message.setData(data);
//                hBaseDAO.insert(message);
//                return false;
//            }
//            return true;
//
//        } catch (HBaseClientException e) {
//            log.error("error validating the data {} exception {}", data, e);
//            return false;
//        }
//
//    }

    @Deprecated //make it thread safe
    public boolean update(String topic, String groupId, String id, byte[] data){
        log.info("updating data {}", data);
        try {
            Message message = new Message();
            message.setGroupId(groupId);
            message.setTopic(topic);
            message.setId(id);
            message.setData(data);
            hBaseDAO.update(message);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while updating the data {} exception {}", data, e);
            return false;
        }
    }


    public boolean deleteRow(String topic, String groupId){
        log.info("deleting data for topic {} and groupId {}", topic, groupId);
        try {
            hBaseDAO.deleteRow(topic, groupId);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }
    }

    public boolean checkAndDeleteRow(String topic, String groupId, long version) {
            log.info("deleting data for topic {} and groupId {}", topic, groupId);
            try {
                hBaseDAO.checkAndDeleteRow(topic, groupId, version);
                return true;
            } catch (HBaseClientException e) {
                log.error("error while replaying the data {}", e);
                return false;
            }

        }
    public boolean checkAndDeleteRow(String rowKey, long version) {
        log.info("deleting data for rowKey {}", rowKey);
        try {
            hBaseDAO.checkAndDeleteRow(rowKey, version);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }

    }


    public boolean deleteEvent(String topic, String groupId, String column) {
        log.info("deleting data for topic {}, groupId {} and column {}", topic, groupId, column);
        try {
            hBaseDAO.deleteColumns(topic, groupId, Lists.newArrayList(column));
            return true;
        } catch (HBaseClientException e) {
            log.error("error while deleting the data {}", e);
            return false;
        }
    }

//    public Map<String, byte[]> search(String prefix) throws HBaseClientException {
//        log.info("searching for prefix {}", prefix);
//        Map<String, byte[]> map = new HashMap<>();
//        List<Result> results = hBaseDAO.search(prefix);
//        for (Result result : results){
//            NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(CF));
//            for (byte[] bytes : navigableMap.keySet()){
//                String key = Bytes.toString(bytes);
//                map.put(key, navigableMap.get(bytes));
//            }
//        }
//        return map;
//    }

    public ArrayList<Result> scan(String firstRow, String topology, int batch) throws HBaseClientException {
        return hBaseDAO.scan(firstRow, topology, batch);
    }
}
