package cloudera.se.fraud.demo.service;

import cloudera.se.fraud.demo.model.TravelResultPOJO;
import cloudera.se.fraud.demo.model.TravelScorePOJO;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;


// to remove

import java.util.Random;

/**
 * Created by jholoman on 9/28/14.
 */
public class TravelScoreService {

    private static final Logger log = Logger.getLogger(TravelScoreService.class);
    public static TravelResultPOJO calcTravelScore(TravelScorePOJO pojo) throws Exception {
        String[] location1 = pojo.getLocation1().split("\\,");
        String[] location2 = pojo.getLocation2().split("\\,");
        double lat1 = Double.parseDouble(location1[0]);
        double lat2 = Double.parseDouble(location2[0]);
        double lon1 = Double.parseDouble(location1[1]);
        double lon2 = Double.parseDouble(location2[1]);
        String t1 = pojo.getTime1();
        String t2 = pojo.getTime2();

        long elapsedSecs = getTimeDifference(t1,t2);
        double distance = getDistance(lat1, lon1, lat2, lon2);
        int score = processScore(elapsedSecs,distance);

       TravelResultPOJO result = new TravelResultPOJO();

        result.setScore(score);
        result.setDistance(distance);
        result.setElapsedSec(elapsedSecs);
        return result;

    }

    private static int processScore(long elapsedSecs, double distance) {

        /** TODO a real calculation **/
        // Speed = Distance(miles) ÷ Time(seconds) * 24 * 60

        int score;
        double mph = distance / (elapsedSecs/3600);
        log.info("distance: " + distance);
        log.info("elapsedSec: " + elapsedSecs);
        log.info("mph: " + mph);
        double stddev = 6;
        double mean = 45;
        double z = (mph-mean)/stddev;

        if (z < 0) {  //mph < 45 we just approve it
          score=1;}
        else if(z > 0 && z <= 3 ) {
            score = 2;
        }
        else if (z > 3  && z <= 6) {
            score = 3;
        } else { score=4; }

        log.info("z: " + z);
        return score;

    }

    private static double degreesToRadians(double degrees) {
        return (degrees * Math.PI / 180.0);
    }
    private static double radiansToDegrees(double radians) {
        return (radians * 180 / Math.PI);
    }

    private static double getDistance(double lat1, double lon1, double lat2, double lon2) {
        double diff = lon1 - lon2;
        double dist = Math.sin(degreesToRadians(lat1)) * Math.sin(degreesToRadians(lat2)) + Math.cos(degreesToRadians(lat1)) * Math.cos(degreesToRadians(lat2)) * Math.cos(degreesToRadians(diff));
        dist = Math.acos(dist);
        dist = radiansToDegrees(dist);
        dist = dist * 60 * 1.1515;

        return round(dist,2);

    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    private static long getTimeDifference(String t1, String t2) {
        SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
        Date d1 = null;
        Date d2 = null;
        try {
            d1 = format.parse(t1);
            d2 = format.parse(t2);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long diff = d2.getTime() - d1.getTime();
        return TimeUnit.MILLISECONDS.toSeconds(diff);
    }
}

