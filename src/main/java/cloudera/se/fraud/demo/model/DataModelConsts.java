package cloudera.se.fraud.demo.model;

/**
 * Created by jholoman on 9/27/14.
 */
import org.apache.hadoop.hbase.util.Bytes;

public class DataModelConsts {
    // Customer table
    public static final byte[] CUSTOMER_TABLE = "customer".getBytes();

    public static final byte[] CUSTOMER_COLUMN_FAMILY = Bytes.toBytes("c");
    public static final byte[] C_NAME_COL = Bytes.toBytes("cn");
    public static final byte[] C_LAT_COL = Bytes.toBytes("clat");
    public static final byte[] C_LON_COL = Bytes.toBytes("clon");
    public static final byte[] LAST_20L_COL = Bytes.toBytes("ltl");
    public static final byte[] LAST_20A_COL = Bytes.toBytes("lta");
    public static final byte[] LAST_TXN_AMOUNT_COL = Bytes.toBytes("ltxna");
    public static final byte[] LAST_TXN_LAT_COL = Bytes.toBytes("ltxnlat");
    public static final byte[] LAST_TXN_LON_COL = Bytes.toBytes("ltxnlon");
    public static final byte[] LAST_TXN_TIME = Bytes.toBytes("ltxnt");
    public static final byte[] TOTAL_SPENT_COL = Bytes.toBytes("ts");
    public static final byte[] AVG_SPENT_COL = Bytes.toBytes("as");
    public static final byte[] TOTAL_TXNS_COL = Bytes.toBytes("tt");

    // Store table
    public static final byte[] STORE_TABLE = Bytes.toBytes("store");
    public static final byte[] STORE_COLUMN_FAMILY = Bytes.toBytes("s");
    public static final byte[] STORE_NAME_COL = Bytes.toBytes("sn");
    public static final byte[] STORE_ADDRESS_COL = Bytes.toBytes("sa");
    public static final byte[] STORE_LOCATION_COL = Bytes.toBytes("slo");
    public static final byte[] STORE_PHONE_COL = Bytes.toBytes("sp");
    public static final byte[] STORE_MERCHANT_TYPE_COL = Bytes.toBytes("smt");
    public static final byte[] STORE_MCC_COL = Bytes.toBytes("smcc");

}


