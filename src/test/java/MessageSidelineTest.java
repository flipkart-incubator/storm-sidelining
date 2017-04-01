import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.configs.HBaseClientConfig;
import com.flipkart.message.sidelining.configs.HBaseTableConfig;

import com.flipkart.message.sidelining.factories.SidelineFactory;

import com.flipkart.message.sidelining.service.SidelineService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.message.sidelining.configs.HBaseTableConfig.CF;

/**
 * Created by saurabh.jha on 21/09/16.
 */
public class MessageSidelineTest {
    private static final Logger log = LoggerFactory.getLogger(MessageSidelineTest.class);

    //table name
    public static final String TABLE_NAME = "oms.messages";
    private static Configuration configuration;
    private static SidelineService service;

    @BeforeClass
    public static void initialize() throws HBaseClientException {
        configuration = HBaseConfiguration.create();
        HTablePool tablePool = new HTablePool(configuration, HBaseClientConfig.poolSize);
        service = SidelineFactory.getService(tablePool, TABLE_NAME);
        createTable();
    }

//    @Test
//    public void testForSideline() {
//        String topic = "test";
//        String groupId = "123";
//        service.sideline(topic, groupId, "id1", "hi".getBytes());
//        Assert.assertFalse(service.validateAndUpdate(topic, groupId, "id2", "hello".getBytes()));
//        Assert.assertTrue(service.validateAndUpdate(topic, groupId + "1", "id1", "hi".getBytes()));
//    }
//
//    @Test
//    public void testForBatchSideline() {
//        String topic = "test";
//        String groupId = "123";
//        Map<String, byte[]> map = new HashMap<>();
//        map.put("id1", "hi".getBytes());
//        map.put("id2", "hello".getBytes());
//        service.forceSideline(topic, groupId, map);
//        Assert.assertFalse(service.validateAndUpdate(topic, groupId, "id3", "check1".getBytes()));
//        Assert.assertTrue(service.validateAndUpdate(topic, groupId + "1", "id1", "hi".getBytes()));
//
//    }

//    @Test
//    public void testForReplay()throws Exception {
//        String topic = "test";
//        String groupId = "123";
//        service.sideline(topic, groupId, "id1", "hi".getBytes());
//        service.sideline(topic, groupId, "id2", "hello".getBytes());
//        Map<String, byte[]> data = service.fetchData(topic, groupId);
//        Assert.assertEquals(Bytes.toString(data.get("id1")), "hi");
//        Assert.assertEquals(Bytes.toString(data.get("id2")), "hello");
//
//        service.deleteRow(topic, groupId, new ArrayList<>(data.keySet()));
//        Assert.assertTrue(service.validateAndUpdate(topic, groupId, "id1", "hi".getBytes()));
//    }
//
//    @Test
//    public void testForPartialReplay() throws Exception {
//        String topic = "test";
//        String groupId = "123";
//        service.sideline(topic, groupId, "id1", "hi".getBytes());
//        service.sideline(topic, groupId, "id2", "hello".getBytes());
//        Map<String, byte[]> data = service.fetchData(topic, groupId);
//        Assert.assertEquals(Bytes.toString(data.get("id1")), "hi");
//        Assert.assertEquals(Bytes.toString(data.get("id2")), "hello");
//
//        List<String> list = new ArrayList<>();
//        list.add("id1");
//        service.deleteRow(topic, groupId);
//        Assert.assertFalse(service.validateAndUpdate(topic, groupId, "id2", "hello".getBytes()));
//    }

//    @Test
//    public void testDeleteRow() throws Exception {
//        String topic = "test";
//        String groupId = "123";
//        Map<String, byte[]> map = new HashMap<>();
//        map.put("id1", "hi".getBytes());
//        map.put("id2", "hello".getBytes());
//        service.sideline(topic, groupId, map);
//        Map<String, byte[]> data = service.fetchData(topic, groupId);
//        Assert.assertEquals(Bytes.toString(data.get("id1")), "hi");
//        Assert.assertEquals(Bytes.toString(data.get("id2")), "hello");
//
//        service.deleteRow(topic, groupId);
//
//        data = service.fetchData(topic, groupId);
//        Assert.assertTrue(data == null || data.size() == 0);
//        Assert.assertTrue(service.validateAndUpdate(topic, groupId, "id2", "hello".getBytes()));
//
//    }

//    @Test
//    public void testPrefixScan() throws Exception {
//        String topic = "test";
//        String groupId = "123";
//        Map<String, byte[]> map = new HashMap<>();
//        map.put("id1", "hi".getBytes());
//        map.put("id2", "hello".getBytes());
//        service.forceSideline(topic, groupId, map);
//
//        Map<String, byte[]> data = service.search("tes");
//        Assert.assertEquals("hi", Bytes.toString(data.get("id1")));
//        Assert.assertEquals("hello", Bytes.toString(data.get("id2")));
//    }

//    @Test
//    public void testForUpdate() throws Exception {
//        String topic = "test";
//        String groupId = "123";
//        Map<String, byte[]> map = new HashMap<>();
//        map.put("id1", "hi".getBytes());
//        map.put("id2", "hello".getBytes());
//        service.sideline(topic, groupId, map);
//        Map<String, byte[]> data = service.fetchData(topic, groupId);
//        Assert.assertEquals(Bytes.toString(data.get("id1")), "hi");
//        Assert.assertEquals(Bytes.toString(data.get("id2")), "hello");
//
//        service.update(topic, groupId, "id1", "check1".getBytes());
//        service.update(topic, groupId, "id2", "check2".getBytes());
//        data = service.fetchData(topic, groupId);
//        Assert.assertEquals(Bytes.toString(data.get("id1")), "check1");
//        Assert.assertFalse("hello".equals(Bytes.toString(data.get("id2"))));
//    }

    @AfterClass
    public static void deleteTable(){
        try {
            // Instantiating HBaseAdmin class
            HBaseAdmin admin = new HBaseAdmin(configuration);
            // disabling table named emp
            admin.disableTable(TABLE_NAME);
            // Deleting emp
            admin.deleteTable(TABLE_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(){
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
            HColumnDescriptor attr = new HColumnDescriptor(CF);
            HColumnDescriptor data = new HColumnDescriptor(CF);
            tableDescriptor.addFamily(attr);
            tableDescriptor.addFamily(data);
            admin.createTable(tableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
