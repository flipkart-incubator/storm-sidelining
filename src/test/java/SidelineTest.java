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
import org.junit.Test;

import java.io.IOException;

/**
 * Created by saurabh.jha on 21/09/16.
 */
@Slf4j
public class SidelineTest {

    @Test
    public void testForSideline() throws Exception {
//        Configuration configuration = HBaseConfiguration.create();
//        createTable(configuration);
//        HTablePool tablePool = new HTablePool(configuration, HBaseClientConfig.poolSize);
//        MessageService service = MessageFactory.getService(tablePool);
//        service.forceSideline("test", "1234", "test", "hi".getBytes());
    }

    private void createTable(Configuration configuration){
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(HBaseTableConfig.TABLE_NAME);
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



    private Configuration initialise() throws HBaseClientException {

        log.info("initializing HBase pool");

        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, HBaseClientConfig.zookeeperQuorum);
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, HBaseClientConfig.port.toString());
        config.set(HConstants.ZK_SESSION_TIMEOUT, Integer.toString(60000));
        config.set("zookeeper.recovery.retry", Integer.toString(3));
        config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, Integer.toString(3));
        config.set(HConstants.HBASE_RPC_TIMEOUT_KEY, Integer.toString(5000));

        return config;
    }

}
