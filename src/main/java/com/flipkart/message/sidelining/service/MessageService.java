package com.flipkart.message.sidelining.service;

import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.configs.HBaseClientConfig;
import com.flipkart.message.sidelining.configs.HBaseTableConfig;
import com.flipkart.message.sidelining.dao.HBaseDAO;
import com.flipkart.message.sidelining.models.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
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

    public MessageService(HTablePool tablePool) {
        this.client = new HBaseClient(tablePool);
        hBaseDAO = new HBaseDAO();
    }

    public boolean forceSideline(String topic, String groupId, String id, byte[] data){
        try {
            Message message = new Message();
            message.setGroupId(groupId);
            message.setTopic(topic);
            message.setId(id);
            message.setData(data);
            hBaseDAO.insert(client, message);
        } catch (HBaseClientException e) {
            log.error("error processing message {}", e);
            return false;
        }
        return true;
    }

    public boolean forceSideline(String topic, String groupId, Map<String, byte[]> map){
        try {
            hBaseDAO.insert(client, topic, groupId, map);
        } catch (HBaseClientException e) {
            log.error("error processing message {}", e);
            return false;
        }
        return true;
    }

    public Map<String, byte[]> fetchData(String topic, String groupId) throws HBaseClientException {
        Map<String, byte[]> map = new HashMap<>();
        Result result = hBaseDAO.get(client, topic, groupId);
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
        try {
            Result result = hBaseDAO.get(client, topic, groupId);
            NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(HBaseTableConfig.COL_FAMILY_DATA));
            if (navigableMap != null && navigableMap.size() > 0){
                Message message = new Message();
                message.setId(id);
                message.setTopic(topic);
                message.setGroupId(groupId);
                message.setData(data);
                hBaseDAO.insert(client, message);
                return false;
            }
            return true;

        } catch (HBaseClientException e) {
            log.error("error validating the data {}", e);
            return false;
        }

    }

    public boolean update(String topic, String groupId, String id, byte[] data){

        try {
            Message message = new Message();
            message.setGroupId(groupId);
            message.setTopic(topic);
            message.setId(id);
            message.setData(data);
            hBaseDAO.update(client, message);
            return true;
        } catch (HBaseClientException e) {
            return false;
        }
    }

    public boolean replay(String topic, String groupId, List<String> ids){
        try {
            hBaseDAO.deleteColumns(client, topic, groupId, ids);
            Result result = hBaseDAO.get(client, topic, groupId);
            NavigableMap<byte[], byte[]> navigableMap  = result.getFamilyMap(Bytes.toBytes(HBaseTableConfig.COL_FAMILY_DATA));
            if (navigableMap == null || navigableMap.size() <= 0)
                hBaseDAO.deleteRow(client, topic, groupId);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while processing callback for columns {}", ids);
            return false;
        }
    }

    public Map<String, byte[]> search(String prefix) throws HBaseClientException {
        Map<String, byte[]> map = new HashMap<>();
        List<Result> results = hBaseDAO.search(client, prefix);
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
