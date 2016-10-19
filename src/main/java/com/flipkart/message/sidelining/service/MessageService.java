package com.flipkart.message.sidelining.service;

import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.configs.HBaseTableConfig;
import com.flipkart.message.sidelining.dao.HBaseDAO;
import com.flipkart.message.sidelining.models.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * Created by saurabh.jha on 16/09/16.
 */
@Slf4j
public class MessageService {

    HBaseClient client;
    HBaseDAO hBaseDAO;
    String tableName;

    public MessageService(HTablePool tablePool, String tableName) {
        this.client = new HBaseClient(tablePool);
        hBaseDAO = new HBaseDAO();
        this.tableName = tableName;
    }

    public boolean forceSideline(String topic, String groupId, String id, byte[] data){
        log.info("sidelining data {} for topic {} and groupId {}", data, topic, groupId);
        try {
            Message message = new Message();
            message.setGroupId(groupId);
            message.setTopic(topic);
            message.setId(id);
            message.setData(data);
            hBaseDAO.insert(client, message, tableName);
        } catch (HBaseClientException e) {
            log.error("error sidelining message {} exception {}", data, e);
            return false;
        }
        return true;
    }

    public boolean forceSideline(String topic, String groupId, Map<String, byte[]> map){
        log.info("sidelining data in batch for topic {} and groupId {}", topic, groupId);
        try {
            hBaseDAO.insert(client, topic, groupId, map, tableName);
        } catch (HBaseClientException e) {
            log.error("error batch sidelining messages {}", e);
            return false;
        }
        return true;
    }

    public Map<String, byte[]> fetchData(String topic, String groupId) throws HBaseClientException {
        log.info("fetching data for topic {} and groupId {}", topic, groupId);
        Map<String, byte[]> map = new HashMap<>();
        Result result = hBaseDAO.get(client, topic, groupId, tableName);
        NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(HBaseTableConfig.COL_FAMILY_DATA));
        if (navigableMap == null || navigableMap.size() == 0)
            return map;
        for (byte[] bytes : navigableMap.keySet()){
            String key = Bytes.toString(bytes);
            map.put(key, navigableMap.get(bytes));
        }
        return map;
    }

    public boolean validateAndUpdate(String topic, String groupId, String id, byte[] data) {
        log.info("validaing data {} for topic {} and groupId {}", data, topic, groupId);
        try {
            Result result = hBaseDAO.get(client, topic, groupId, tableName);
            NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(HBaseTableConfig.COL_FAMILY_DATA));
            if (navigableMap != null && navigableMap.size() > 0){
                Message message = new Message();
                message.setId(id);
                message.setTopic(topic);
                message.setGroupId(groupId);
                message.setData(data);
                hBaseDAO.insert(client, message, tableName);
                return false;
            }
            return true;

        } catch (HBaseClientException e) {
            log.error("error validating the data {} exception {}", data, e);
            return false;
        }

    }

    public boolean update(String topic, String groupId, String id, byte[] data){
        log.info("updating data {}", data);
        try {
            Message message = new Message();
            message.setGroupId(groupId);
            message.setTopic(topic);
            message.setId(id);
            message.setData(data);
            hBaseDAO.update(client, message, tableName);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while updating the data {} exception {}", data, e);
            return false;
        }
    }

    public boolean deleteData(String topic, String groupId, List<String> ids){
        log.info("deleting data for topic {} and groupId {}", topic, groupId);
        try {
            hBaseDAO.deleteColumns(client, topic, groupId, ids, tableName);
            Result result = hBaseDAO.get(client, topic, groupId, tableName);
            NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(HBaseTableConfig.COL_FAMILY_DATA));
            if (navigableMap == null || navigableMap.size() == 0)
                hBaseDAO.deleteRow(client, topic, groupId, tableName);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }
    }

    public boolean deleteRow(String topic, String groupId){
        log.info("deleting data for topic {} and groupId {}", topic, groupId);
        try {
            hBaseDAO.deleteRow(client, topic, groupId, tableName);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }
    }

    public Map<String, byte[]> search(String prefix) throws HBaseClientException {
        log.info("searching for prefix {}", prefix);
        Map<String, byte[]> map = new HashMap<>();
        List<Result> results = hBaseDAO.search(client, prefix, tableName);
        for (Result result : results){
            NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(HBaseTableConfig.COL_FAMILY_DATA));
            for (byte[] bytes : navigableMap.keySet()){
                String key = Bytes.toString(bytes);
                map.put(key, navigableMap.get(bytes));
            }
        }
        return map;
    }
}
