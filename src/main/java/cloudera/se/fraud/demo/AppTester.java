package cloudera.se.fraud.demo;
import cloudera.se.fraud.demo.service.HbaseFraudService;
import cloudera.se.fraud.demo.model.CustomerPOJO;
import cloudera.se.fraud.demo.model.StorePOJO;
import org.apache.log4j.Logger;

/**
 * Created by jholoman on 9/28/14.
 */
public class AppTester {
    private static final Logger log = Logger.getLogger(AppTester.class);

    public static void main (String[] args) throws Exception{
    Long pan = 4556308909411460L;
    int merchantId = 993;

        HbaseFraudService service = new HbaseFraudService();
        log.info("Calling Customer");
        CustomerPOJO cust = service.getCustomerFromHBase(pan);
        log.info("Customer done");
        log.info("Calling Store");
        StorePOJO store = service.getStoreFromHBase(merchantId);
        log.info("Store done");
        System.out.println(cust.getName());
        System.out.println(store.getName());
    }
}
