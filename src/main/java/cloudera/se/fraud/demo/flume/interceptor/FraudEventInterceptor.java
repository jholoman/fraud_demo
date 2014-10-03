package cloudera.se.fraud.demo.flume.interceptor;
/**
 * Created by jholoman on 9/30/14.
 */
import cloudera.se.fraud.demo.model.CustomerPOJO;
import cloudera.se.fraud.demo.model.StorePOJO;
import cloudera.se.fraud.demo.model.TravelResultPOJO;
import cloudera.se.fraud.demo.model.TravelScorePOJO;
import cloudera.se.fraud.demo.service.HbaseFraudService;
import cloudera.se.fraud.demo.service.TravelScoreService;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class FraudEventInterceptor implements Interceptor {

    private static final Logger log = Logger.getLogger(FraudEventInterceptor.class);
    private static final String d = "|";

    private HbaseFraudService hbaseFraudService;
    private TravelScoreService travelScoreService;
    public FraudEventInterceptor() { }

    /**
     * Any initialization / startup needed by the Interceptor.
     */
    @Override
    public void initialize()
    {
        hbaseFraudService = new HbaseFraudService();
        travelScoreService = new TravelScoreService();
        log.info("Starting Initialize");
    }

    /**
     * Interception of a single {@link Event}.
     * @param event Event to be intercepted
     * @return Original or modified event, or {@code null} if the Event
     * is to be dropped (i.e. filtered out).
     */
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        Pattern p = Pattern.compile("\\|+");

        String txn = Bytes.toString(event.getBody());
        String[] tokens = p.split(txn);

        String txn_id = tokens[0];
        long customerId = Long.parseLong(tokens[1]);
        String txn_time = tokens[2];
        double txn_amount = Double.parseDouble(tokens[3]);
        int merchantId = Integer.parseInt(tokens[4]);

        CustomerPOJO customer = new CustomerPOJO();
        StorePOJO store = new StorePOJO();

        headers.put("CustomerRowKey",String.valueOf(customerId));

        try {
            customer = hbaseFraudService.getCustomerFromHBase(customerId);
            store = hbaseFraudService.getStoreFromHBase(merchantId);
        } catch (IOException e) {
        }

        TravelScorePOJO score = new TravelScorePOJO();
        TravelResultPOJO result = new TravelResultPOJO();


        score.setLocation1(nvl(customer.getLastTransactionLat(),customer.getHomeLat()).concat(",").concat(nvl(customer.getLastTransactionLon(),customer.getHomeLon())));
        score.setLocation2(store.getLocation());
        score.setTime1(customer.getLastTransactionTime());
        score.setTime2(txn_time);

        try {
            result = travelScoreService.calcTravelScore(score);
        } catch (Exception e) {
            e.printStackTrace();
        }
        StringBuilder buf = new StringBuilder();
        //txn_id 0, customer_id 1, txn_time 2, txn_amount 3, txn_location 4, merchant_id 5, store_name 6
        //store_address 7, store_type 8, store_mcc 9, customer_name 10, cust_lat 11, cust_lat 12
        //cust_last_txn_amount 13, last_tran_lat 14, last_tran_lon 15, last_tran_time 16, total_spent 17
        //avg_spent 18, txn_count 19, score 20, distance 21, elapsed_Sec 22

        buf.append(txn_id).append(d).append(String.valueOf(customerId)).append(d).append(txn_time).append(d);
        buf.append(txn_amount).append(d).append(store.getLocation()).append(d);
        //Store
        buf.append(merchantId).append(d).append(store.getName()).append(d).append(store.getAddress()).append(d);
        buf.append(store.getMerchantType()).append(d).append(store.getMcc()).append(d);
        //Customer fields
        buf.append(customer.getName()).append(d);
        buf.append(customer.getHomeLat()).append(d).append(customer.getHomeLon()).append(customer.getLastTransactionAmount()).append(d);
        buf.append(customer.getLastTransactionLat()).append(d).append(customer.getLastTransactionLon()).append(d).append(customer.getLastTransactionTime()).append(d);
        buf.append(customer.getTotalSpent()).append(d).append(customer.getAvgSpent()).append(d);
        buf.append(customer.getTransactionCount()).append(d).append(result.getScore()).append(d);
        buf.append(result.getDistance()).append(d).append(result.getElapsedSec());

        String message = buf.toString();
        event.setBody(message.getBytes());
        buf.setLength(0);
        return event;
    }

    /**
     * Interception of a batch of {@linkplain Event events}.
     * @param events Input list of events
     * @return Output list of events. The size of output list MUST NOT BE GREATER
     * than the size of the input list (i.e. transformation and removal ONLY).
     * Also, this method MUST NOT return {@code null}. If all events are dropped,
     * then an empty List is returned.
     */
    public List<Event> intercept(List<Event> events){
        List<Event> result = new ArrayList<Event>();
        for (Event event:events) {
            intercept(event);

        }
        return events;
    }

    /**
     * Perform any closing / shutdown needed by the Interceptor.
     */
    public void close() {
        log.info("closing interceptor");
    }

    public <T> T nvl(T a, T b) {
        return (a == null)?b:a;
    }
    /** Builder implementations MUST have a no-arg constructor */

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new FraudEventInterceptor();
        }
        @Override
        public void configure(Context context) {}
    }

}
