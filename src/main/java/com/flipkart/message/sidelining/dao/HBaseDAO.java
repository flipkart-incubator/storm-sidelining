package com.flipkart.message.sidelining.dao;

import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.hbase.KeyDistributor;
import com.flipkart.message.sidelining.models.Message;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        long version = client.incrementVersion(tableName, message.getRowKey(), CF, VERSION);
        version = version + 1L;
        Map<String, byte[]> cells = Maps.newHashMap();
        cells.put(message.getId(), message.getData());
        cells.put(VERSION,Bytes.toBytes(version));
        client.putColumns(tableName, message.getRowKey(), CF, cells);
    }

    public boolean checkAndPut(Message message, long version) throws HBaseClientException {
        version = client.incrementVersion(tableName, message.getRowKey(), CF, VERSION);
        return client.checkAndPutColumn(tableName, message.getRowKey(), CF, message.getId(), message.getData(), VERSION, Bytes.toBytes(version));
    }

    public void deleteRow(String topic, String groupId) throws HBaseClientException {
        client.clearRow(tableName, getRowKey(topic, groupId));
    }

    public void checkAndDeleteRow(String topic, String groupId, long version) throws HBaseClientException {
        client.checkAndClearRow(tableName, getRowKey(topic, groupId),CF,VERSION,version);
    }

    public void checkAndDeleteRow(String rowKey, long version) throws HBaseClientException {
        client.checkAndClearRow(tableName, rowKey, CF, VERSION, version);
    }

    public void deleteColumns(String topic, String groupId, List<String> ids) throws HBaseClientException {
        client.deleteColumns(tableName, getRowKey(topic, groupId), CF, ids);
    }

    public void deleteColumn(String rowKey, String column) throws HBaseClientException {
        client.deleteColumns(tableName, rowKey, CF, Lists.newArrayList(column));
    }

    public Result get(String topic, String groupId) throws HBaseClientException {
        return client.getRow(tableName, topic + groupId);
    }

    public void update(Message message) throws HBaseClientException {
        client.updateColumn(tableName, message.getRowKey(), CF, message.getId(), message.getData());
    }

    public ArrayList<Result> scan(String firstRow, String prefix, int batch) throws HBaseClientException {
        return client.scanPrefix(tableName, firstRow, prefix, batch);
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
