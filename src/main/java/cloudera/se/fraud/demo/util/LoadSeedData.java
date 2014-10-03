package cloudera.se.fraud.demo.util;

import cloudera.se.fraud.demo.model.DataModelConsts;
import cloudera.se.fraud.demo.model.CustomerPOJO;
import cloudera.se.fraud.demo.model.StorePOJO;
import java.security.MessageDigest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.io.InputStreamReader;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;


/**
 * Created by jholoman on 9/27/14.
 */
public class LoadSeedData {

    private static final Logger log = Logger.getLogger(LoadSeedData.class);

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println("LoadSeedData {customer|store|both}");
            return;
        }
        String table = args[0];


        if (table.equals("customer")) {
            loadCustomers("customers_final.txt");
        } else if (table.equals("store")){
           loadStores("stores_final.txt") ;
        } else if (table.equals("both")) {
            loadCustomers("customers_final.txt");
            loadStores("stores_final.txt");
        }
    }


    public static void loadStores(String filename) throws Exception, IOException {
        boolean result = createProfileTable(DataModelConsts.STORE_TABLE, DataModelConsts.STORE_COLUMN_FAMILY);
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF8"));

        String line;
        StringBuilder buf = new StringBuilder();
        ArrayList<StorePOJO> storeList = new ArrayList<StorePOJO>();

        while ((line = br.readLine()) != null) {
            String[] data = line.split("\\|");
            System.out.println(data[0]);

            int merchantId = Integer.parseInt(data[0]);
            String name = data[1];
            String address = data[2];
            String lat = data[3];
            String lon = data[4];
            String phone = data[5];
            String merchantType = data[6];
            String mcc = data[7];
            String storeLocation = lat + "," + lon;

            StorePOJO store = new StorePOJO();
            store.setMerchantId(merchantId);
            store.setName(name);
            store.setAddress(address);
            store.setLocation(storeLocation);
            store.setPhone(phone);
            store.setMerchantType(merchantType);
            store.setMcc(mcc);

            buf.append(merchantId).append("|").append(address).append("|").append(phone);
            buf.append("|").append(lat).append("|").append(lon).append("|").append(phone);
            buf.append("|").append(merchantType).append("|").append(mcc);
            log.info(buf);
            buf.setLength(0);

            storeList.add(store);
        }
           br.close();

        insertStores(storeList);

    }
    public static void loadCustomers(String filename) throws Exception,IOException {
        boolean result = createProfileTable(DataModelConsts.CUSTOMER_TABLE,DataModelConsts.CUSTOMER_COLUMN_FAMILY);
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        StringBuilder buf  = new StringBuilder();
        ArrayList<CustomerPOJO> customerList = new ArrayList<CustomerPOJO>();

        while ((line = br.readLine()) != null) {
            String[] data = line.split("\\|");
            System.out.println(data[0]);

            String pan = data[0];
            String name = data[1];
            log.info("The name is  " + name);
            String lat = data[2];
            String lon = data[3];

            CustomerPOJO cust = new CustomerPOJO();
            cust.setCustomerId(Long.parseLong(pan));
            cust.setName(name);
            cust.setHomeLat(lat);
            cust.setHomeLon(lon);
            customerList.add(cust);
        }
        br.close();


        insertCustomers(customerList);


    }

    public static boolean createProfileTable(byte[] tableName, byte[] columnFamily) throws Exception,IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hbase = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        if (hbase.tableExists(tableName)) {
                hbase.disableTable(tableName);
                hbase.deleteTable(tableName);
        }

        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
        desc.addFamily(family);
        hbase.createTable(desc);
      return true;
    }

    public static void insertCustomers(ArrayList<CustomerPOJO> pojoList)
            throws Exception,IOException {


        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf,DataModelConsts.CUSTOMER_TABLE);

        ArrayList<Row> actions = new ArrayList<Row>();

        for (CustomerPOJO pojo: pojoList) {

            byte [] rowKey = "0".getBytes();
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");

            rowKey =  md.digest(Bytes.toBytes(pojo.getCustomerId()));
            } catch (NoSuchAlgorithmException e){}

            Put put = new Put(rowKey);
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.C_NAME_COL, Bytes.toBytes(pojo.getName())); //+ "|" + pojo.getHomeLocation() + "|" +  System.currentTimeMillis()));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.C_LAT_COL, Bytes.toBytes(pojo.getHomeLat()));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.C_LON_COL, Bytes.toBytes(pojo.getHomeLon()));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_AMOUNT_COL, Bytes.toBytes(0.00 ));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.AVG_SPENT_COL, Bytes.toBytes(0.00));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_SPENT_COL, Bytes.toBytes(0.00));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_TXNS_COL, Bytes.toBytes(0.00));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_20A_COL, Bytes.toBytes(""));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_20L_COL, Bytes.toBytes(""));

            actions.add(put);
        }

        Object[] results = new Object[actions.size()];
        try
        {
            table.batch(actions, results);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }


    public static void insertStores(ArrayList<StorePOJO> pojoList)
            throws Exception,IOException {


        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf,DataModelConsts.STORE_TABLE);

        ArrayList<Row> actions = new ArrayList<Row>();

        for (StorePOJO pojo: pojoList) {

            //int salt = pojo.getMerchantId() % 4;
            //byte[] rowKey =  Bytes.toBytes(StringUtils.leftPad(Integer.toString(salt), 0, "0") + "-" + pojo.getMerchantId() );
            byte[] rowKey = Bytes.toBytes(pojo.getMerchantId());

            Put put = new Put(rowKey);
            put.add(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_NAME_COL, Bytes.toBytes(pojo.getName()));
            put.add(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_ADDRESS_COL, Bytes.toBytes(pojo.getAddress()));
            put.add(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_LOCATION_COL, Bytes.toBytes(pojo.getLocation() ));
            put.add(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_PHONE_COL, Bytes.toBytes(pojo.getPhone()));
            put.add(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_MERCHANT_TYPE_COL, Bytes.toBytes(pojo.getMerchantType()));
            put.add(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_MCC_COL, Bytes.toBytes(pojo.getMcc()));

            actions.add(put);
        }

        Object[] results = new Object[actions.size()];
        try
        {
            table.batch(actions, results);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }




}
