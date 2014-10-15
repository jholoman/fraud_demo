package cloudera.se.fraud.demo.flume.interceptor;
/**
 * Created by jholoman on 9/30/14.
 */
import cloudera.se.fraud.demo.model.*;
import cloudera.se.fraud.demo.service.HbaseFraudService;
import cloudera.se.fraud.demo.service.TravelScoreService;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

public class FraudEventInterceptor implements Interceptor {

    private static final Logger log = Logger.getLogger(FraudEventInterceptor.class);
    private static final String d = "|";

    private HbaseFraudService hbaseFraudService;
    private TravelScoreService travelScoreService;
    public FraudEventInterceptor() { }

    static ExecutorService executorService = Executors.newFixedThreadPool(20);

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

    private String convertToJSON (FinalTransactionPOJO finalTxn) {

        ObjectWriter ow = new ObjectMapper().writer();
        String json = null;
        try {
           json = ow.writeValueAsString(finalTxn);

        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();

        }
        return json;
    }
    /**
     * Interception of a single {@link Event}.
     * @param event Event to be intercepted
     * @return Original or modified event, or {@code null} if the Event
     * is to be dropped (i.e. filtered out).
     */
    @Override
    public Event intercept(Event event) {
        log.debug("Intercepting event");
       // Map<String, String> headers = event.getHeaders();
        Pattern p = Pattern.compile("\\|+");
        String txn = Bytes.toString(event.getBody());
        String[] tokens = p.split(txn);

        String txnId = tokens[0];
        long customerId = Long.parseLong(tokens[1]);
        String txnTime = tokens[2];
        double txnAmount = Double.parseDouble(tokens[3]);
        int merchantId = Integer.parseInt(tokens[4]);
        String txnLat = null;
        String txnLon = null;

        CustomerPOJO customer = null;
        StorePOJO store = null;

        try {
            log.debug("Getting Customer " + customerId);
            customer = hbaseFraudService.getCustomerFromHBase(customerId, txnId);
            log.debug("Getting Store " + merchantId);
            store = hbaseFraudService.getStoreFromHBase(merchantId, txnId);
            String[] location = store.getLocation().split("\\,");
            txnLat = location[0];
            txnLon = location[1];
        } catch (IOException e) {
            log.debug("Error at line 79");
            log.debug(e);
        }

        TravelScorePOJO score = new TravelScorePOJO();
        TravelResultPOJO result = null;

        score.setLocation1(nvl(customer.getLastTransactionLat(), customer.getHomeLat()).concat(",").concat(nvl(customer.getLastTransactionLon(), customer.getHomeLon())));
        score.setLocation2(store.getLocation());
        score.setTime1(customer.getLastTransactionTime());
        score.setTime2(txnTime);

        try {
            result = travelScoreService.calcTravelScore(score);
        } catch (Exception e) {
            log.debug("Error at line 93");
            log.debug(e);
            e.printStackTrace();
        }
        String authResult = "N";
        String alertYN = "N";
        switch (result.getScore()) {
            case 1:  authResult = "Y";
                     alertYN = "N";
                break;
            case 2:  authResult = "Y";
                     alertYN = "N";
                break;
            case 3:  authResult = "Y";
                     alertYN = "Y";
                break;
            case 4:  authResult = "N";
                     alertYN = "Y";
                break;
        }
        FinalTransactionPOJO finalTxn = new FinalTransactionPOJO(
          txnId, customer.getRowKey(), txnTime, txnAmount, merchantId, txnLat, txnLon, customer.getLastTransactionAmount(),
          customer.getLastTransactionLat(), customer.getLastTransactionLon(), customer.getLastTransactionTime(),
          result.getElapsedSec(), result.getDistance(), result.getScore(), authResult, alertYN);

        // Update the customer with the new values for the transaction
        customer.setLastTransactionAmount(txnAmount);
        customer.setLastTransactionLat(txnLat);
        customer.setLastTransactionLon(txnLon);
        customer.setLastTransactionTime(txnTime);
        customer.setLast20Amounts(HbaseFraudService.pushValue(String.valueOf(txnAmount), customer.getLast20Amounts()));
        customer.setLast20Locations(HbaseFraudService.pushValue(txnLat +"|"+ txnLon, customer.getLast20Locations()));


        try { log.debug("Saving Customer");
            hbaseFraudService.saveCustomerToHbase(customer, txnId); }
        catch (IOException e) {
            log.debug("Error at line 116");
            log.debug(e);
        }
            /*TODO avro or json */

        //String message = convertToJSON(finalTxn);
        String message = finalTxn.toString2();
        event.setBody(message.getBytes());

        log.debug("returning event");
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
    public List<Event> intercept(List<Event> events) {
        log.info("Starting Interceptor Batch");
        List<Event> result = new ArrayList<Event>();
        Context c = new Context();

        ArrayList<Callable<Event>> callableList = new ArrayList<Callable<Event>>();

        for (final Event event : events) {
            callableList.add(new Callable<Event>() {
                @Override
                public Event call() {
                    intercept(event);
                    return event;
                }
            });
        }
        try {
            List<Future<Event>> futures = executorService.invokeAll(callableList);
        } catch (InterruptedException e) {
            log.debug(e);
        }
        log.info("Ending Interceptor Batch");

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
