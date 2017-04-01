package com.flipkart.message.sidelining.dao;

import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.hbase.KeyDistributor;
import com.flipkart.message.sidelining.models.Message;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

import static com.flipkart.message.sidelining.configs.HBaseTableConfig.CF;
import static com.flipkart.message.sidelining.configs.HBaseTableConfig.VERSION;

/**
 * Created by saurabh.jha on 19/09/16.
 */
public class HBaseDAO {

    private HBaseClient client;
    private static final KeyDistributor keyDistributor = new KeyDistributor.MurmurKeyDistributor(512);
    private String tableName;

    public HBaseDAO(HBaseClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }


    public void insert(Message message) throws HBaseClientException {
        client.putColumn(tableName, message.getRowKey(), CF, message.getId(), message.getData());
        client.incrementVersion(tableName, message.getRowKey(), CF, VERSION);
    }

    public boolean checkAndPut(Message message, long version) throws HBaseClientException {
        boolean success = client.checkAndPutColumn(tableName, message.getRowKey(), CF, message.getId(), message.getData(), VERSION, Bytes.toBytes(version));
        if (success) {
            client.incrementVersion(tableName, message.getRowKey(), CF, VERSION);
        }
        return success;
    }

    public void deleteRow(String topic, String groupId) throws HBaseClientException {

        client.clearRow(tableName, getRowKey(topic, groupId));
    }

    public Result get(String topic, String groupId) throws HBaseClientException {
        return client.getRow(tableName, topic + groupId);
    }

    public void update(Message message) throws HBaseClientException {
        client.updateColumn(tableName, message.getRowKey(), CF, message.getId(), message.getData());
    }

    public void deleteColumns(String topic, String groupId, List<String> ids) throws HBaseClientException {
        client.deleteColumns(tableName, getRowKey(topic, groupId), CF, ids);
    }

    public List<Result> search(String prefix) throws HBaseClientException {
        return client.scanPrefix(tableName, prefix);
    }

    public long getVersion(String topic, String groupId) throws HBaseClientException {
        byte[] version = client.getColumnForRow(tableName, getRowKey(topic, groupId), CF, VERSION);
        if(version == null) return 0;
        return Bytes.toLong(version);
    }

    public static String getRowKey(String topic, String groupId) {
        return _prepareRowKey(createRowKey(topic, groupId));
    }

    private static String _prepareRowKey(String rowKey) {
        return keyDistributor.distributorEnrich(rowKey);
    }

    private static String createRowKey(String topic, String groupId) {
        return topic + ":" + groupId;
    }
}
