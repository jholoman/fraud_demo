package cloudera.se.fraud.demo.service;

import cloudera.se.fraud.demo.model.DataModelConsts;
import cloudera.se.fraud.demo.model.CustomerPOJO;
import cloudera.se.fraud.demo.model.StorePOJO;

import cloudera.se.fraud.demo.model.TravelScorePOJO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.codec.digest.DigestUtils;
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

    public static String getHashedRowKey(long rowKey) {
      byte[] hashedRowKey = null;
        String sRowKey = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            hashedRowKey =  md.digest(Bytes.toBytes(rowKey));
            sRowKey = DigestUtils.md5Hex(hashedRowKey);
            log.debug(sRowKey);
        } catch (NoSuchAlgorithmException e){}
        return sRowKey;
    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public static String pushValue (String newValue, String oldValues) {

        LinkedList<String> ll = new LinkedList<String>(Arrays.asList(oldValues.split(",")));
        ll.addFirst(newValue);
        if (ll.size() > 20) {
            ll.subList(20, ll.size()).clear();
        }
        String returnValue = ll.toString();
        returnValue = returnValue.replace("[", "").replace("]", "");
        return returnValue;
    }

    public CustomerPOJO getCustomerFromHBase(long pan, String txn) throws IOException {
        log.debug("getting htable for " + txn);
        HTable table = new HTable(conf, DataModelConsts.CUSTOMER_TABLE);
        log.debug("got new htable for " + txn);
        String rowKey = getHashedRowKey(pan);
        log.debug("got rowKey for " + txn);
        Get get = new Get(Bytes.toBytes(rowKey));
        log.debug("calling get on customer for " + txn);
        Result r = table.get(get);
        log.debug("got customer row for " + txn);

        try {

            String name = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.C_NAME_COL));
            String homeLat = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.C_LAT_COL));
            String homeLon = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.C_LON_COL));
            //double lastTransactionAmount = Bytes.toDouble(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_AMOUNT_COL));
            String LTA = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY,DataModelConsts.LAST_TXN_AMOUNT_COL));
            String lastTransactionLat = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_LAT_COL));
            String lastTransactionLon = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_LON_COL));
            String lastTransactionTime = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_TIME));
            //double totalSpent = Bytes.toDouble(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_SPENT_COL));
            String TS = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_SPENT_COL));
            //double avgSpent = Bytes.toDouble(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.AVG_SPENT_COL));
            String AS = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.AVG_SPENT_COL));
            //int transactionCount = Bytes.toInt(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_TXNS_COL));
            String TC = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_TXNS_COL));
            String last20Locations = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_20L_COL));
            String last20Amounts = Bytes.toString(r.getValue(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_20A_COL));


       double lastTransactionAmount = round(Double.parseDouble(LTA),2);
       log.debug(lastTransactionAmount);
       double totalSpent = round(Double.parseDouble(TS), 2);
       log.debug(TS);
       double avgSpent = round(Double.parseDouble(AS),2);
       log.debug(AS);
       int transactionCount = Integer.parseInt(TC);
       log.debug(TC);


            CustomerPOJO pojo = new CustomerPOJO(pan, name, homeLat, homeLon, lastTransactionAmount, lastTransactionLat, lastTransactionLon, lastTransactionTime,
                    totalSpent, avgSpent, transactionCount, last20Locations, last20Amounts, rowKey);
            log.debug("returning customer pojo for " + txn);

            return pojo;
        } catch (Exception e) {
            log.debug(e);
        }
        log.debug("creating new pojo for " + txn);
        return null;
    }
    public StorePOJO getStoreFromHBase(int merchantId, String txn) throws IOException {
        log.debug("getting store htable for " + txn);
        HTable table = new HTable(conf, DataModelConsts.STORE_TABLE);
        log.debug("got new store htable for " + txn);
        Get get = new Get(Bytes.toBytes(merchantId));
        log.debug("calling get for " + txn);
        Result r = table.get(get);
        log.debug("got customer row for  " + txn);

        String name = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_NAME_COL));
        String address = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_ADDRESS_COL));
        String location = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_LOCATION_COL));
        String phone = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_PHONE_COL));
        String merchantType = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_MERCHANT_TYPE_COL));
        String mcc = Bytes.toString(r.getValue(DataModelConsts.STORE_COLUMN_FAMILY, DataModelConsts.STORE_MCC_COL));


        log.debug("creating new store pojo for " + txn);
        StorePOJO pojo = new StorePOJO(merchantId, name, address, location, phone, merchantType, mcc);
        log.debug("returning store pojo for " + txn);
        return pojo;
    }

    public void saveCustomerToHbase(CustomerPOJO pojo, String txn_id) throws IOException {
        log.debug("Starting Save to Hbase");
        Configuration conf = HBaseConfiguration.create();
        log.debug("getting customer_save htbale for  " + txn_id);
        HTable table = new HTable(conf,DataModelConsts.CUSTOMER_TABLE);

            String rowKey = HbaseFraudService.getHashedRowKey(pojo.getCustomerId());
            int totalTxns = pojo.getTransactionCount();
            totalTxns ++;

            double avgSpent = round(pojo.getTotalSpent() + pojo.getLastTransactionAmount() / totalTxns,2);
            double totalSpent = round(pojo.getTotalSpent() + pojo.getLastTransactionAmount(),2);

        /*TODO implement last 20 logic */
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_TIME, Bytes.toBytes(pojo.getLastTransactionTime()));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_AMOUNT_COL, Bytes.toBytes(String.valueOf(pojo.getLastTransactionAmount())));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_LAT_COL, Bytes.toBytes(pojo.getLastTransactionLat()));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_TXN_LON_COL, Bytes.toBytes(pojo.getLastTransactionLon()));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.AVG_SPENT_COL, Bytes.toBytes(String.valueOf(avgSpent)));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_SPENT_COL, Bytes.toBytes(String.valueOf(totalSpent)));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.TOTAL_TXNS_COL, Bytes.toBytes(String.valueOf(totalTxns)));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_20A_COL, Bytes.toBytes(pojo.getLast20Amounts()));
            put.add(DataModelConsts.CUSTOMER_COLUMN_FAMILY, DataModelConsts.LAST_20L_COL, Bytes.toBytes(pojo.getLast20Locations()));
            log.debug("Doing insert for "+ txn_id);
            table.put(put);
            log.debug("Inserted row for" + txn_id);
    }
}
