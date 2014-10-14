package cloudera.se.fraud.demo;
import cloudera.se.fraud.demo.model.TravelResultPOJO;
import cloudera.se.fraud.demo.model.TravelScorePOJO;
import cloudera.se.fraud.demo.service.HbaseFraudService;
import cloudera.se.fraud.demo.service.TravelScoreService;
import cloudera.se.fraud.demo.model.CustomerPOJO;
import cloudera.se.fraud.demo.model.StorePOJO;
import org.apache.log4j.Logger;

/**
 * Created by jholoman on 9/28/14.
 */
public class AppTester {
    private static final Logger log = Logger.getLogger(AppTester.class);

    public static <T> T nvl(T a, T b) {
        return (a == null)?b:a;
    }
    public static void main (String[] args) throws Exception{
    Long pan = 4556308909411460L;
    int merchantId = 1760;
        String txn_time = "2014-10-09 15:42:22";
        TravelScorePOJO score = new TravelScorePOJO();
        TravelResultPOJO result = null;

        HbaseFraudService service = new HbaseFraudService();
        log.info("Calling Customer");
        CustomerPOJO cust = service.getCustomerFromHBase(pan,"1");
        log.info("Customer done");
        log.info("Calling Store");
        StorePOJO store = service.getStoreFromHBase(merchantId,"1");
        log.info("Store done");
        System.out.println(cust.getName());
        System.out.println(store.getName());

        score.setLocation1(nvl(cust.getLastTransactionLat(), cust.getHomeLat()).concat(",").concat(nvl(cust.getLastTransactionLon(), cust.getHomeLon())));
        score.setLocation2(store.getLocation());
        score.setTime1(cust.getLastTransactionTime());
        score.setTime2(txn_time);

        log.info(score.getLocation1());
        log.info(score.getLocation2());
        log.info(score.getTime1());
        log.info(score.getTime2());

        TravelResultPOJO result1  = new TravelResultPOJO();
        try {
            result1 = TravelScoreService.calcTravelScore(score);
        } catch (Exception e) {
            log.debug("Error at line 93");
            log.debug(e);
            e.printStackTrace();
        }


        String val = HbaseFraudService.pushValue("t23", "t22,t21,t20,t19,t18,t17,t16,t15,t14,t13,t12,t11,t10,t9,t8,t7,t6,t5,t4,t3,t2,t1,");
        log.info(val);

    }
}
