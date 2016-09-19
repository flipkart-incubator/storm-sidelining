package com.flipkart.message.sidelining.service;

import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.configs.HBaseClientConfig;
import com.flipkart.message.sidelining.dao.HBaseDAO;
import com.flipkart.message.sidelining.models.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by saurabh.jha on 16/09/16.
 */
@Slf4j
public class MessageService {

    HBaseClient client;
    HBaseDAO hBaseDAO;

    public MessageService(Configuration config, int poolSize) throws HBaseClientException {
        this.client = new HBaseClient(config, poolSize);
        hBaseDAO = new HBaseDAO();
    }

    public boolean forceSideline(String topic, String groupId, String id, String key, byte[] data){
        try {
            Message message = new Message();
            message.setGroupId(groupId);
            message.setTopic(topic);
            message.setId(id);
            message.setData(data);
            hBaseDAO.insert(message);
        } catch (HBaseClientException e) {
            log.error("error processing message {}", e);
            return false;
        }
        return true;
    }

    public boolean forceSideline(String topic, String groupId, Map<String, byte[]> map){
        try {
            hBaseDAO.insert(topic, groupId, map);
        } catch (HBaseClientException e) {
            log.error("error processing message {}", e);
            return false;
        }
        return true;
    }

    public Map<String, byte[]> replay(String topic, String groupId){
        Map<String, byte[]> map = new HashMap<>();
        return map;
    }

    public void callBack(String topic, String groupId, List<String> messageIds){

    }


    private void initialise() throws HBaseClientException {

        log.info("initializing HBase client");

        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, HBaseClientConfig.zookeeperQuorum);
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, HBaseClientConfig.port.toString());
        config.set(HConstants.ZK_SESSION_TIMEOUT, Integer.toString(60000));
        config.set("zookeeper.recovery.retry", Integer.toString(3));
        config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, Integer.toString(3));
        config.set(HConstants.HBASE_RPC_TIMEOUT_KEY, Integer.toString(5000));

        this.client = new HBaseClient(config,HBaseClientConfig.poolSize);
    }
}
