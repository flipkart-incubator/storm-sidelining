package com.flipkart.message.sidelining.client;

/**
 * Created by saurabh.jha on 18/09/16.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class HBaseClient {

    private HTablePool tablePool;
    private Configuration config;

    public HBaseClient(HTablePool tablePool) {
        this.tablePool = tablePool;
    }

    public HBaseClient(Configuration config){
        this.config = config;
    }

    public HTablePool getTablePool() {
        return tablePool;
    }

    public void putColumn(String tableName, String row, String cf, String col, byte[] value) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {

            Put p = new Put(Bytes.toBytes(row));
            p.add(Bytes.toBytes(cf), Bytes.toBytes(col), value);
            table.put(p);
        } catch (IOException e) {
            String msg = "While inserting [" + row + ": " + cf + ": " + col + "]";
            throw new HBaseClientException(msg, e);
        }
    }

    public void putColumns(String tableName, String row, String cf, Map<String, byte[]> columns) throws HBaseClientException {
        try (HTableInterface table = tablePool.getTable(tableName)) {
            Put p = new Put(Bytes.toBytes(row));
            byte[] cfBytes = Bytes.toBytes(cf);
            for (Map.Entry<String, byte[]> column : columns.entrySet()) {
                p.add(cfBytes, Bytes.toBytes(column.getKey()), column.getValue());
            }
            table.put(p);
        } catch (IOException e) {
            String msg = "While inserting columns into [" + row + ": " + cf +  "]";
            throw new HBaseClientException(msg, e);
        }
    }

    public void deleteColumns(String tableName, String row, String cf, List<String> columnIds) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(row));
            byte[] cfBytes = Bytes.toBytes(cf);
            for (String id : columnIds){
                delete.deleteColumns(cfBytes, Bytes.toBytes(id));
            }
            table.delete(delete);
        } catch (IOException e) {
            String msg = "While deleting columns  ";
            throw new HBaseClientException(msg, e);
        }
    }

    public void updateColumn(String tableName, String row, String cf, String col, byte [] val) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            // Instantiating Put class
            //accepts a row name
            Put p = new Put(Bytes.toBytes(row));
            // Updating a cell value
            p.add(Bytes.toBytes(cf), Bytes.toBytes(col), val);
            table.put(p);
        } catch (IOException e) {
            String msg = "While mutate columns  ";
            throw new HBaseClientException(msg, e);

        }
    }

    public boolean checkAndPutColumn(String tableName, String row, String cf, String col, byte[] value, String checkColumn, byte[] checkValue)
            throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Put p = new Put(Bytes.toBytes(row));
            p.add(Bytes.toBytes(cf), Bytes.toBytes(col), value);
            return table.checkAndPut(Bytes.toBytes(row), Bytes.toBytes(cf), Bytes.toBytes(checkColumn), checkValue, p);
        } catch (IOException e) {
            String msg = "While check and inserting [" + row + ": " + cf + ": " + col + "]";
            throw new HBaseClientException(msg, e);
        }
    }

    public boolean checkAndPutColumns(String tableName, String row, String cf, Map<String, byte[]> columns, String checkColumn, byte[] checkValue)
            throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Put p = new Put(Bytes.toBytes(row));
            for (Map.Entry<String, byte[]> column : columns.entrySet()) {
                p.add(Bytes.toBytes(cf), Bytes.toBytes(column.getKey()), column.getValue());
            }
            return table.checkAndPut(Bytes.toBytes(row), Bytes.toBytes(cf), Bytes.toBytes(checkColumn), checkValue, p);
        } catch (IOException e) {
            String msg = "While check and inserting columns into [" + row + ": " + cf +  "]";
            throw new HBaseClientException(msg, e);
        }
    }

    public Result[] getRows(String tableName, List<String> rows, String cf) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            List<Get> gets = new ArrayList<>();
            for (String row : rows) {
                Get get = new Get(Bytes.toBytes(row));
                get.addFamily(Bytes.toBytes(cf));
                gets.add(get);
            }
            Result[] results = table.get(gets);
            return results;
        } catch (IOException e) {
            String msg = "While reading [" + rows.toString() + ": " + cf +"]";
            throw new HBaseClientException(msg, e);
        }
    }

    public Result getRow(String tableName, String row, String cf) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(row));
            get.addFamily(Bytes.toBytes(cf));
            Result result = table.get(get);
            return result;
        } catch (IOException e) {
            String msg = "While reading [" + row + ": " + cf +"]";
            throw new HBaseClientException(msg, e);
        }
    }

    public Result getRow(String tableName, String row) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(row));
            Result result = table.get(get);
            return result;
        } catch (IOException e) {
            String msg = "While reading [" + row + "]";
            throw new HBaseClientException(msg, e);
        }
    }

    //TODO Improve the interface
    public Result[] scan(String tableName, String row, String stopRow, Filter filter, String cf) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Scan scan;
            if(row != null && stopRow == null){
                scan = new Scan(row.getBytes());
            }else if(row != null && stopRow != null){
                scan = new Scan(row.getBytes(), stopRow.getBytes());
            }else{
                scan = new Scan();
            }
            scan.addFamily(cf.getBytes());
            if(filter != null){
                scan.setFilter(filter);
            }
            ResultScanner resultScanner = table.getScanner(scan);
            ArrayList<Result> resultSets = Lists.newArrayList();
            for(Result r : resultScanner) {
                resultSets.add(r);
            }
            return resultSets.toArray(new Result[resultSets.size()]);
        } catch (IOException e) {
            String msg = "While reading [" + row + ": " + cf +"]";
            throw new HBaseClientException(msg, e);
        }
    }

    public List<Result> scanRows(String tableName, String row, String stopRow, String cf, List<String> qualifiers) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Scan scan = new Scan(row.getBytes(), stopRow.getBytes());
            if ( qualifiers != null && qualifiers.size() > 0 ) {
                for ( String qualifier : qualifiers )
                    scan.addColumn(cf.getBytes(), qualifier.getBytes());
            }
            ResultScanner resultScanner = table.getScanner(scan);

            ArrayList<Result> resultSets = Lists.newArrayList();
            for(Result r : resultScanner) {
                //                r.get
                resultSets.add(r);
            }
            return resultSets;
        } catch (IOException e) {
            String msg = "While reading [" + row + ": " + cf +"]";
            throw new HBaseClientException(msg, e);
        }
    }

    public List<Result> scanPrefix(String tableName, String prefix) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Scan scan = new Scan(Bytes.toBytes(prefix));
            PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(prefix));
            scan.setFilter(prefixFilter);
            ResultScanner resultScanner = table.getScanner(scan);
            ArrayList<Result> resultSets = Lists.newArrayList();
            for(Result r : resultScanner) {
                resultSets.add(r);
            }
            return resultSets;
        } catch (IOException e) {
            String msg = "While searching for prefix " + prefix;
            throw new HBaseClientException(msg, e);
        }

    }


    public Result getColumnsForRow(String tableName, String row, String cf, List<String> columns) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(row));
            for (String column : columns) {
                get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column));
            }
            Result result = table.get(get);
            return result;
        } catch (IOException e) {
            String msg = "While reading [" + row + ": " + cf +"]";
            throw new HBaseClientException(msg, e);
        }
    }

    public byte[] getColumnForRow(String tableName, String row, String cf, String column) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(row));
            get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column));
            Result result = table.get(get);
            return result.getValue(Bytes.toBytes(cf), Bytes.toBytes(column));
        } catch (IOException e) {
            String msg = "While reading [" + row + ": " + cf +"]";
            throw new HBaseClientException(msg, e);
        }
    }

    public void clearRow(String tableName, String row) throws HBaseClientException {
        try ( HTableInterface table = tablePool.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(row));
            table.delete(delete);
        } catch (IOException e) {
            String msg = "While deleting [" + row + "]";
            throw new HBaseClientException(msg, e);
        }
    }

    public void createTable(HTableDescriptor descriptor) throws HBaseClientException {
        try (HBaseAdmin admin = new HBaseAdmin(config)) {
            admin.createTable(descriptor);
        } catch (IOException e) {
            throw new HBaseClientException(e);
        }
    }

    public void modifyTable(String tableName, HTableDescriptor descriptor) throws HBaseClientException {
        try (HBaseAdmin admin = new HBaseAdmin(config)) {
            admin.disableTable(tableName);
            admin.modifyTable(Bytes.toBytes(tableName), descriptor);
            admin.enableTable(tableName);
        } catch (IOException e) {
            throw new HBaseClientException(e);
        }
    }
}

