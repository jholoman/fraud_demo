package cloudera.se.fraud.demo.model;

public class TransactionPOJO {

    String transactionId;

    String customerId;

    String transactionTime;

    double amount;

    int merchantId;

    public TransactionPOJO() {
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

    public double getAmount() {
        return this.amount;
    }

    public void setAmount(double value) {
        this.amount = value;
    }

    public int getMerchantId() {
        return this.merchantId;
    }

    public void setMerchantId(int value) {
        this.merchantId = value;
    }
}
