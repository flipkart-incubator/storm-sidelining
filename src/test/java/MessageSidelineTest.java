import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.configs.HBaseClientConfig;
import com.flipkart.message.sidelining.configs.HBaseTableConfig;
import com.flipkart.message.sidelining.factories.MessageFactory;
import com.flipkart.message.sidelining.service.MessageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by saurabh.jha on 21/09/16.
 */
@Slf4j
public class MessageSidelineTest {

    //table name
    public static final String TABLE_NAME = "oms.messages";
    private static Configuration configuration;
    private static MessageService service;

    @BeforeClass
    public static void initialise() throws HBaseClientException {
        configuration = HBaseConfiguration.create();
        HTablePool tablePool = new HTablePool(configuration, HBaseClientConfig.poolSize);
        service = MessageFactory.getService(tablePool);
        createTable();
    }

    @Test
    public void testForSideline() {
        String topic = "test";
        String groupId = "123";
        service.forceSideline(topic, groupId, "id1", "hi".getBytes(), TABLE_NAME);
        Assert.assertFalse(service.validateAndUpdate(topic, groupId, "id2", "hello".getBytes(), TABLE_NAME));
        Assert.assertTrue(service.validateAndUpdate(topic, groupId + "1", "id1", "hi".getBytes(), TABLE_NAME));
    }

    @Test
    public void testForBatchSideline() {
        String topic = "test";
        String groupId = "123";
        Map<String, byte[]> map = new HashMap<>();
        map.put("id1", "hi".getBytes());
        map.put("id2", "hello".getBytes());
        service.forceSideline(topic, groupId, map, TABLE_NAME);
        Assert.assertFalse(service.validateAndUpdate(topic, groupId, "id3", "check1".getBytes(), TABLE_NAME));
        Assert.assertTrue(service.validateAndUpdate(topic, groupId + "1", "id1", "hi".getBytes(), TABLE_NAME));

    }

    @Test
    public void testForReplay()throws Exception {
        String topic = "test";
        String groupId = "123";
        Map<String, byte[]> map = new HashMap<>();
        map.put("id1", "hi".getBytes());
        map.put("id2", "hello".getBytes());
        service.forceSideline(topic, groupId, map, TABLE_NAME);
        Map<String, byte[]> data = service.fetchData(topic, groupId, TABLE_NAME);
        Assert.assertEquals(Bytes.toString(data.get("id1")), "hi");
        Assert.assertEquals(Bytes.toString(data.get("id2")), "hello");

        service.deleteData(topic, groupId, new ArrayList<>(data.keySet()), TABLE_NAME);
        Assert.assertTrue(service.validateAndUpdate(topic, groupId, "id1", "hi".getBytes(), TABLE_NAME));
    }

    @Test
    public void testForPartialReplay() throws Exception {
        String topic = "test";
        String groupId = "123";
        Map<String, byte[]> map = new HashMap<>();
        map.put("id1", "hi".getBytes());
        map.put("id2", "hello".getBytes());
        service.forceSideline(topic, groupId, map, TABLE_NAME);
        Map<String, byte[]> data = service.fetchData(topic, groupId, TABLE_NAME);
        Assert.assertEquals(Bytes.toString(data.get("id1")), "hi");
        Assert.assertEquals(Bytes.toString(data.get("id2")), "hello");

        List<String> list = new ArrayList<>();
        list.add("id1");
        service.deleteData(topic, groupId, list, TABLE_NAME);
        Assert.assertFalse(service.validateAndUpdate(topic, groupId, "id2", "hello".getBytes(), TABLE_NAME));
    }

    @Test
    public void testDeleteRow() throws Exception {
        String topic = "test";
        String groupId = "123";
        Map<String, byte[]> map = new HashMap<>();
        map.put("id1", "hi".getBytes());
        map.put("id2", "hello".getBytes());
        service.forceSideline(topic, groupId, map, TABLE_NAME);
        Map<String, byte[]> data = service.fetchData(topic, groupId, TABLE_NAME);
        Assert.assertEquals(Bytes.toString(data.get("id1")), "hi");
        Assert.assertEquals(Bytes.toString(data.get("id2")), "hello");

        service.deleteRow(topic, groupId, TABLE_NAME);

        data = service.fetchData(topic, groupId, TABLE_NAME);
        Assert.assertTrue(data == null || data.size() == 0);
        Assert.assertTrue(service.validateAndUpdate(topic, groupId, "id2", "hello".getBytes(), TABLE_NAME));

    }

    @Test
    public void testPrefixScan() throws Exception {
        String topic = "test";
        String groupId = "123";
        Map<String, byte[]> map = new HashMap<>();
        map.put("id1", "hi".getBytes());
        map.put("id2", "hello".getBytes());
        service.forceSideline(topic, groupId, map, TABLE_NAME);

        Map<String, byte[]> data = service.search("tes", TABLE_NAME);
        Assert.assertEquals("hi", Bytes.toString(data.get("id1")));
        Assert.assertEquals("hello", Bytes.toString(data.get("id2")));
    }

    @Test
    public void testForUpdate() throws Exception {
        String topic = "test";
        String groupId = "123";
        Map<String, byte[]> map = new HashMap<>();
        map.put("id1", "hi".getBytes());
        map.put("id2", "hello".getBytes());
        service.forceSideline(topic, groupId, map, TABLE_NAME);
        Map<String, byte[]> data = service.fetchData(topic, groupId, TABLE_NAME);
        Assert.assertEquals(Bytes.toString(data.get("id1")), "hi");
        Assert.assertEquals(Bytes.toString(data.get("id2")), "hello");

        service.update(topic, groupId, "id1", "check1".getBytes(), TABLE_NAME);
        service.update(topic, groupId, "id2", "check2".getBytes(), TABLE_NAME);
        data = service.fetchData(topic, groupId, TABLE_NAME);
        Assert.assertEquals(Bytes.toString(data.get("id1")), "check1");
        Assert.assertFalse("hello".equals(Bytes.toString(data.get("id2"))));
    }

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
            HColumnDescriptor attr = new HColumnDescriptor(HBaseTableConfig.COL_FAMILY_ATTRIBUTES);
            HColumnDescriptor data = new HColumnDescriptor(HBaseTableConfig.COL_FAMILY_DATA);
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
