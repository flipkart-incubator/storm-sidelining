package com.flipkart.storm.sidelining.hbase.key;

/**
 * Created by gupta.rajat on 27/07/17.
 */
public interface KeyFormatter {
    public String getRowKey(String topic, String groupId);

    class HbaseKeyFormatter implements KeyFormatter {

        private static final KeyDistributor keyDistributor = new KeyDistributor.MurmurKeyDistributor(512);

        private static String _prepareRowKey(String rowKey) {
            return keyDistributor.distributorEnrich(rowKey);
        }

        private static String createRowKey(String topic, String groupId) {
            return topic + ":" + groupId;
        }

        @Override
        public String getRowKey(String topic, String groupId) {
            return _prepareRowKey(createRowKey(topic, groupId));
        }

    }
}
