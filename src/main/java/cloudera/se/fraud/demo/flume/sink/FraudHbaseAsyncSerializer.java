package cloudera.se.fraud.demo.flume.sink;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;

import cloudera.se.fraud.demo.model.DataModelConsts;
import cloudera.se.fraud.demo.model.EnrichedTransactionPOJO;
import cloudera.se.fraud.demo.service.HbaseFraudService;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.apache.flume.conf.ComponentConfiguration;
/**
        * A serializer for the AsyncHBaseSink, which splits the event body into
        * multiple columns and inserts them into a row
        * the headers
        */
public class FraudHbaseAsyncSerializer implements AsyncHbaseEventSerializer {
    private byte[] table;
    private byte[] colFam;
    private Event currentEvent;
    private byte[][] columnNames;
    private final List<PutRequest> puts = new ArrayList<PutRequest>();
    private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
    private byte[] currentRowKey;
    private final byte[] eventCountCol = "eventCount".getBytes();
    private EnrichedTransactionPOJO eTxn = new EnrichedTransactionPOJO();

    private static final Logger log = Logger.getLogger(FraudHbaseAsyncSerializer.class);


    @Override
    public void initialize(byte[] table, byte[] cf) {
        this.table = table;
        this.colFam = cf;
        log.info("Starting Serializer");
    }

    @Override
    public void setEvent(Event event) {
        // Set the event and verify that the rowKey is not present
        this.currentEvent = event;
        String rowKeyStr = currentEvent.getHeaders().get("CustomerRowKey");
        if (rowKeyStr == null) {
            throw new FlumeException("No row key found in headers!");
        }
        currentRowKey = HbaseFraudService.getHashedRowKey(Long.parseLong(rowKeyStr));
    }

    @Override
    public List<PutRequest> getActions() {
        // Split the event body and get the values for the columns
       // Pattern p = Pattern.compile("\\|+");
        //#Pattern t = Pattern.compile()
        String txn = Bytes.toString(currentEvent.getBody());
        String[] tokens = StringUtils.splitPreserveAllTokens(txn,"|");
        puts.clear();

        //txn_id 0, customer_id 1, txn_time 2, txn_amount 3, txn_location 4, merchant_id 5, store_name 6
        //store_address 7, store_type 8, store_mcc 9, customer_name 10, cust_lat 11, cust_lat 12
        //cust_last_txn_amount 13, last_tran_lat 14, last_tran_lon 15, last_tran_time 16, total_spent 17
        //avg_spent 18, txn_count 19, score 20, distance 21, elapsed_Sec 22

        log.debug(tokens[0] + "-" + tokens[1]);
        log.debug(txn);
        if (log.isDebugEnabled()) {
            for (int i = 0; i < tokens.length; i++) {
                log.info(i + " " + tokens[i]);
            }
        }
        //update counters
        double txnAmount = Double.parseDouble(tokens[3]);
        double totalSpent = Double.parseDouble(tokens[17]);
        int transactionCount = Integer.parseInt(tokens[19]);
        transactionCount ++;
        totalSpent += txnAmount;
        double avgTxn = totalSpent/transactionCount;

        //Set values
        eTxn.setTransactionTime(tokens[2]);
        eTxn.setTransactionAmount(txnAmount);
        String[] location = tokens[4].split("\\,");
        eTxn.setStoreLat(location[0]);
        eTxn.setStoreLon(location[1]);
        eTxn.setTotalSpent(totalSpent);
        eTxn.setAvgSpent(avgTxn);
        eTxn.setTxnCount(transactionCount);

        //add to putRequest
        puts.add(new PutRequest(table,currentRowKey,colFam, DataModelConsts.LAST_TXN_TIME,Bytes.toBytes(eTxn.getTransactionTime())));
        puts.add(new PutRequest(table,currentRowKey,colFam, DataModelConsts.LAST_TXN_AMOUNT_COL,Bytes.toBytes(eTxn.getTransactionAmount())));
        puts.add(new PutRequest(table,currentRowKey,colFam, DataModelConsts.LAST_TXN_LAT_COL,Bytes.toBytes(eTxn.getStoreLat())));
        puts.add(new PutRequest(table,currentRowKey,colFam, DataModelConsts.LAST_TXN_LON_COL,Bytes.toBytes(eTxn.getStoreLon())));
        puts.add(new PutRequest(table,currentRowKey,colFam, DataModelConsts.TOTAL_SPENT_COL,Bytes.toBytes(eTxn.getTotalSpent())));
        puts.add(new PutRequest(table,currentRowKey,colFam, DataModelConsts.AVG_SPENT_COL,Bytes.toBytes(eTxn.getAvgSpent())));
        puts.add(new PutRequest(table,currentRowKey,colFam, DataModelConsts.TOTAL_TXNS_COL,Bytes.toBytes(eTxn.getTxnCount())));

        return puts;
    }

    @Override
    public List<AtomicIncrementRequest> getIncrements() {
        incs.clear();
        //Increment the number of events received
        incs.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(), colFam, eventCountCol));
        return incs;
    }

    @Override
    public void cleanUp() {
        table = null;
        colFam = null;
        currentEvent = null;
        columnNames = null;
        currentRowKey = null;
        log.info("Leaving Serializer");
    }

    @Override
    public void configure(Context context) {
        //Get the column names from the configuration
        //String cols = new String(context.getString("columns"));
        //String[] names = cols.split(",");
        //byte[][] columnNames = new byte[names.length][];
        //int i = 0;
        //for(String name : names) {
         //   columnNames[i++] = name.getBytes();
        //}
    }

    @Override
    public void configure(ComponentConfiguration conf) {
    }
}


