package cloudera.se.fraud.demo.model;


public class EnrichedTransactionPOJO {

    String transactionId;
    String customerId;
    String transactionTime;
    double transactionAmount;
    int merchantId;
    String storeLat;
    String storeLon;
    String storeName;
    String storeAddress;
    String storeType;
    String storeMCC;
    String cName;
    String cHomeLat;
    String cHomeLong;
    String lastTxnAmount;
    String lastTxnLat;
    String lastTxnLon;
    String lastTxnTime;
    double totalSpent;
    double avgSpent;
    long elapsedSec;
    int score;
    int txnCount;



    public EnrichedTransactionPOJO() {
    }

    public EnrichedTransactionPOJO(String transactionId, String customerId, String transactionTime, double transactionAmount, int merchantId, String storeLat, String storeLon, String storeName, String storeAddress, String storeType, String storeMCC, String cName, String cHomeLat, String cHomeLong, String lastTxnAmount, String lastTxnLat, String lastTxnLon, String lastTxnTime, double totalSpent, double avgSpent, long elapsedSec) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.transactionTime = transactionTime;
        this.transactionAmount = transactionAmount;
        this.merchantId = merchantId;
        this.storeLat = storeLat;
        this.storeLon = storeLon;
        this.storeName = storeName;
        this.storeAddress = storeAddress;
        this.storeType = storeType;
        this.storeMCC = storeMCC;
        this.cName = cName;
        this.cHomeLat = cHomeLat;
        this.cHomeLong = cHomeLong;
        this.lastTxnAmount = lastTxnAmount;
        this.lastTxnLat = lastTxnLat;
        this.lastTxnLon = lastTxnLon;
        this.lastTxnTime = lastTxnTime;
        this.totalSpent = totalSpent;
        this.avgSpent = avgSpent;
        this.elapsedSec = elapsedSec;
    }

    public String getTransactionId() {
        return this.transactionId;
    }

    public void setTransactionId(String value) {
        this.transactionId = value;
    }

    public String getCustomerId() {
        return this.customerId;
    }

    public void setCustomerId(String value) {
        this.customerId = value;
    }

    public String getTransactionTime() {
        return this.transactionTime;
    }

    public void setTransactionTime(String value) {
        this.transactionTime = value;
    }

    public double getTransactionAmount() {
        return this.transactionAmount;
    }

    public void setTransactionAmount(double value) {
        this.transactionAmount = value;
    }

    public int getMerchantId() {
        return this.merchantId;
    }

    public void setMerchantId(int value) {
        this.merchantId = value;
    }

    public String getStoreLat() {
        return this.storeLat;
    }

    public void setStoreLat(String value) {
        this.storeLat = value;
    }

    public String getStoreLon() {
        return this.storeLon;
    }

    public void setStoreLon(String value) {
        this.storeLon = value;
    }

    public String getStoreName() {
        return this.storeName;
    }

    public void setStoreName(String value) {
        this.storeName = value;
    }

    public String getStoreAddress() {
        return this.storeAddress;
    }

    public void setStoreAddress(String value) {
        this.storeAddress = value;
    }

    public String getStoreType() {
        return this.storeType;
    }

    public void setStoreType(String value) {
        this.storeType = value;
    }

    public String getStoreMCC() {
        return this.storeMCC;
    }

    public void setStoreMCC(String value) {
        this.storeMCC = value;
    }

    public String getCName() {
        return this.cName;
    }

    public void setCName(String value) {
        this.cName = value;
    }

    public String getCHomeLat() {
        return this.cHomeLat;
    }

    public void setCHomeLat(String value) {
        this.cHomeLat = value;
    }

    public String getCHomeLong() {
        return this.cHomeLong;
    }

    public void setCHomeLong(String value) {
        this.cHomeLong = value;
    }

    public String getLastTxnAmount() {
        return this.lastTxnAmount;
    }

    public void setLastTxnAmount(String value) {
        this.lastTxnAmount = value;
    }

    public String getLastTxnLat() {
        return this.lastTxnLat;
    }

    public void setLastTxnLat(String value) {
        this.lastTxnLat = value;
    }

    public String getLastTxnLon() {
        return this.lastTxnLon;
    }

    public void setLastTxnLon(String value) {
        this.lastTxnLon = value;
    }

    public String getLastTxnTime() {
        return this.lastTxnTime;
    }

    public void setLastTxnTime(String value) {
        this.lastTxnTime = value;
    }

    public double getTotalSpent() {
        return this.totalSpent;
    }

    public void setTotalSpent(double value) {
        this.totalSpent = value;
    }

    public double getAvgSpent() {
        return this.avgSpent;
    }

    public void setAvgSpent(double value) {
        this.avgSpent = value;
    }

    public long getElapsedSec() {
        return this.elapsedSec;
    }

    public void setElapsedSec(long value) {
        this.elapsedSec = value;
    }


    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
    public int getTxnCount() {
        return txnCount;
    }

    public void setTxnCount(int txnCount) {
        this.txnCount = txnCount;
    }
}
