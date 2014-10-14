package cloudera.se.fraud.demo.model;

/**
 * Created by jholoman on 10/11/14.
 */
public class FinalTransactionPOJO {

    String transactionId;
    String customerId;
    String transactionTime;
    double transactionAmount;
    int merchantId;
    String storeLat;
    String storeLon;
    double lastTxnAmount;
    String lastTxnLat;
    String lastTxnLon;
    String lastTxnTime;
    long elapsedSec;
    double distance;
    double score;
    String authResult;
    String alertYN;

    public FinalTransactionPOJO(String transactionId, String customerId, String transactionTime, double transactionAmount, int merchantId, String storeLat, String storeLon, double lastTxnAmount, String lastTxnLat, String lastTxnLon, String lastTxnTime, long elapsedSec, double distance, double score, String authResult, String alertYN) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.transactionTime = transactionTime;
        this.transactionAmount = transactionAmount;
        this.merchantId = merchantId;
        this.storeLat = storeLat;
        this.storeLon = storeLon;
        this.lastTxnAmount = lastTxnAmount;
        this.lastTxnLat = lastTxnLat;
        this.lastTxnLon = lastTxnLon;
        this.lastTxnTime = lastTxnTime;
        this.elapsedSec = elapsedSec;
        this.distance = distance;
        this.score = score;
        this.authResult = authResult;
        this.alertYN = alertYN;
    }

    public FinalTransactionPOJO(String transactionId, String customerId, String transactionTime, double transactionAmount, int merchantId, String storeLocation, double lastTxnAmount, String lastTxnLat, String lastTxnLon, String lastTxnTime, long elapsedSec, double distance, double score, String authResult, String alertYN) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.transactionTime = transactionTime;
        this.transactionAmount = transactionAmount;
        this.merchantId = merchantId;
        String[] location = storeLocation.split("\\,");
        this.storeLat = location[0];
        this.storeLon = location[1];
        this.lastTxnAmount = lastTxnAmount;
        this.lastTxnLat = lastTxnLat;
        this.lastTxnLon = lastTxnLon;
        this.lastTxnTime = lastTxnTime;
        this.elapsedSec = elapsedSec;
        this.distance = distance;
        this.score = score;
        this.authResult = authResult;
        this.alertYN = alertYN;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(String transactionTime) {
        this.transactionTime = transactionTime;
    }

    public double getTransactionAmount() {
        return transactionAmount;
    }

    public void setTransactionAmount(double transactionAmount) {
        this.transactionAmount = transactionAmount;
    }

    public int getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(int merchantId) {
        this.merchantId = merchantId;
    }

    public String getStoreLat() {
        return storeLat;
    }

    public void setStoreLat(String storeLat) {
        this.storeLat = storeLat;
    }

    public String getStoreLon() {
        return storeLon;
    }

    public void setStoreLon(String storeLon) {
        this.storeLon = storeLon;
    }

    public double getLastTxnAmount() {
        return lastTxnAmount;
    }

    public void setLastTxnAmount(double lastTxnAmount) {
        this.lastTxnAmount = lastTxnAmount;
    }

    public String getLastTxnLat() {
        return lastTxnLat;
    }

    public void setLastTxnLat(String lastTxnLat) {
        this.lastTxnLat = lastTxnLat;
    }

    public String getLastTxnLon() {
        return lastTxnLon;
    }

    public void setLastTxnLon(String lastTxnLon) {
        this.lastTxnLon = lastTxnLon;
    }

    public String getLastTxnTime() {
        return lastTxnTime;
    }

    public void setLastTxnTime(String lastTxnTime) {
        this.lastTxnTime = lastTxnTime;
    }

    public long getElapsedSec() {
        return elapsedSec;
    }

    public void setElapsedSec(long elapsedSec) {
        this.elapsedSec = elapsedSec;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getAuthResult() {
        return authResult;
    }

    public void setAuthResult(String authResult) {
        this.authResult = authResult;
    }

    public String getAlertYN() {
        return alertYN;
    }

    public void setAlertYN(String alertYN) {
        this.alertYN = alertYN;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    @Override
    public String toString() {
        return "TXN[" +
                "transactionId='" + transactionId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", transactionTime='" + transactionTime + '\'' +
                ", transactionAmount=" + transactionAmount +
                ", merchantId=" + merchantId +
                ", storeLat='" + storeLat + '\'' +
                ", storeLon='" + storeLon + '\'' +
                ", lastTxnAmount=" + lastTxnAmount +
                ", lastTxnLat='" + lastTxnLat + '\'' +
                ", lastTxnLon='" + lastTxnLon + '\'' +
                ", lastTxnTime='" + lastTxnTime + '\'' +
                ", elapsedSec=" + elapsedSec +
                ", distance=" + distance +
                ", score=" + score +
                ", authResult='" + authResult + '\'' +
                ", alertYN='" + alertYN + '\'' +
                ']';
    }
    public String toString2() {
        return  transactionId +
                "," + customerId +
                "," + transactionTime +
                "," + transactionAmount +
                "," + merchantId +
                "," + storeLat +
                "," + storeLon +
                "," + lastTxnAmount +
                "," + lastTxnLat +
                "," + lastTxnLon +
                "," + lastTxnTime +
                "," + elapsedSec +
                "," + distance +
                "," + score +
                "," + authResult +
                "," + alertYN  ;
    }
}
