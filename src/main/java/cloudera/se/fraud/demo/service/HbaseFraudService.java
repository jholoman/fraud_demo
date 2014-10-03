package cloudera.se.fraud.demo.service;

import cloudera.se.fraud.demo.model.DataModelConsts;
import cloudera.se.fraud.demo.model.CustomerPOJO;
import cloudera.se.fraud.demo.model.StorePOJO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by jholoman on 9/28/14.
 */
public class HbaseFraudService {
    private static Configuration conf = null;
    private static final Logger log = Logger.getLogger(HbaseFraudService.class);
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
    }

    public static byte[] getHashedRowKey (long rowKey) {
      byte[] hashedRowKey = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            hashedRowKey =  md.digest(Bytes.toBytes(rowKey));
        } catch (NoSuchAlgorithmException e){}
        return hashedRowKey;
    }

   public CustomerPOJO getCustomerFromHBase(long pan) throws IOException {
       HTable table = new HTable(conf, DataModelConsts.CUSTOMER_TABLE);

       byte[] rowKey = getHashedRowKey(pan);

       Get get = new Get(rowKey);
       Result r = table.get(get);

       String name = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY,DataModelConsts.C_NAME_COL));
       String homeLat = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.C_LAT_COL));
       String homeLon = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.C_LON_COL));
       double lastTransactionAmount = Bytes.toDouble(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_AMOUNT_COL));
       String lastTransactionLat = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_LAT_COL));
       String lastTransactionLon = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_LON_COL));
       String lastTransactionTime = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_TIME));
       double totalSpent = Bytes.toDouble(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY,DataModelConsts.TOTAL_SPENT_COL));
       double avgSpent = Bytes.toDouble(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY,DataModelConsts.AVG_SPENT_COL));
       int transactionCount = Bytes.toInt(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_TXNS_COL));
       String last20Locations = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_20L_COL));
       String last20Amounts = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_20A_COL));

       CustomerPOJO pojo = new CustomerPOJO(pan, name, homeLat, homeLon, lastTransactionAmount, lastTransactionLat, lastTransactionLon, lastTransactionTime,
               totalSpent, avgSpent, transactionCount, last20Locations, last20Amounts);

       return pojo;

   }

    public StorePOJO getStoreFromHBase(int merchantId) throws IOException {

        HTable table = new HTable(conf, DataModelConsts.STORE_TABLE);
        Get get = new Get(Bytes.toBytes(merchantId));
        Result r = table.get(get);

        String name = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_NAME_COL));
        String address = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_ADDRESS_COL));
        String location = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_LOCATION_COL));
        String phone = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_PHONE_COL));
        String merchantType = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_MERCHANT_TYPE_COL));
        String mcc = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_MCC_COL));


        StorePOJO pojo = new StorePOJO(merchantId, name, address, location, phone, merchantType, mcc);
        return pojo;

    }
}
