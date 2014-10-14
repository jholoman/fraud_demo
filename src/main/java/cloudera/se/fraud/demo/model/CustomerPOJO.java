package cloudera.se.fraud.demo.model;

public class CustomerPOJO {

    long customerId;
    String name;
    String homeLat;
    String homeLon;
    String last20Locations;
    String last20Amounts;
    double lastTransactionAmount;
    String lastTransactionLat;
    String lastTransactionLon;
    String lastTransactionTime;
    double totalSpent;
    double avgSpent;
    int transactionCount;
    String rowKey;

    public CustomerPOJO() {
    }

    public CustomerPOJO(long customerId,
                        String name,
                        String lat,
                        String lon,
                        double lastTransactionAmount,
                        String lastTransactionLat,
                        String lastTransactionLon,
                        String lastTransactionTime,
                        double totalSpent,
                        double avgSpent,
                        int transactionCount,
                        String last20Locations,
                        String last20Amounts,
                        String rowkey) {

        this.customerId = customerId;
        this.homeLat = lat;
        this.homeLon = lon;
        this.name = name;
        this.lastTransactionAmount = lastTransactionAmount;
        this.lastTransactionLat = lastTransactionLat;
        this.lastTransactionLon = lastTransactionLon;
        this.lastTransactionTime = lastTransactionTime;
        this.totalSpent = totalSpent;
        this.avgSpent = avgSpent;
        this.transactionCount = transactionCount;
        this.last20Locations = last20Locations;
        this.last20Amounts = last20Amounts;
        this.rowKey = rowkey;

        /*
        HashSet<Double> amountHS = new HashSet<Double>();
        StringTokenizer st = new StringTokenizer(last20Amounts, "|");
        while(st.hasMoreTokens()) {
            amountHS.add(Double.parseDouble(st.nextToken()));
        }

        HashSet<String> locationHS = new HashSet<String>();
        StringTokenizer st2 = new StringTokenizer(last20Locations, ",");
        while(st2.hasMoreTokens()) {
            locationHS.add(st2.nextToken());
        }
        */
    }

    public long getCustomerId() {
        return this.customerId;
    }

    public void setCustomerId(long value) {
        this.customerId = value;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String value) {
        this.name = value;
    }

    public String getHomeLat() {
        return this.homeLat;
    }

    public void setHomeLat(String value) {
        this.homeLat = value;
    }

    public String getHomeLon() {
        return this.homeLon;
    }

    public void setHomeLon(String value) {
        this.homeLon = value;
    }

    public String getLast20Locations() {
        return this.last20Locations;
    }

    public void setLast20Locations(String value) {
        this.last20Locations = value;
    }

    public String getLast20Amounts() {
        return this.last20Amounts;
    }

    public void setLast20Amounts(String value) {
        this.last20Amounts = value;
    }

    public Double getLastTransactionAmount() {
        return this.lastTransactionAmount;
    }

    public void setLastTransactionAmount(Double value) {
        this.lastTransactionAmount = value;
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

    public int getTransactionCount() {
        return this.transactionCount;
    }

    public void setTransactionCount(int value) {
        this.transactionCount = value;
    }

    public String getLastTransactionLat() {
        return lastTransactionLat;
    }

    public void setLastTransactionLat(String lastTransactionLat) {
        this.lastTransactionLat = lastTransactionLat;
    }

    public String getLastTransactionLon() {
        return lastTransactionLon;
    }

    public void setLastTransactionLon(String lastTransactionLon) {
        this.lastTransactionLon = lastTransactionLon;
    }

    public String getLastTransactionTime() {
        return this.lastTransactionTime;
    }

    public void setLastTransactionTime(String value) {
        this.lastTransactionTime = value;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String value) {
        this.rowKey = value;
    }

    @Override
    public String toString() {
        return "CustomerPOJO{" +
                "customerId=" + customerId +
                ", name='" + name + '\'' +
                ", homeLat='" + homeLat + '\'' +
                ", homeLon='" + homeLon + '\'' +
                ", last20Locations='" + last20Locations + '\'' +
                ", last20Amounts='" + last20Amounts + '\'' +
                ", lastTransactionAmount=" + lastTransactionAmount +
                ", lastTransactionLat='" + lastTransactionLat + '\'' +
                ", lastTransactionLon='" + lastTransactionLon + '\'' +
                ", lastTransactionTime='" + lastTransactionTime + '\'' +
                ", totalSpent=" + totalSpent +
                ", avgSpent=" + avgSpent +
                ", transactionCount=" + transactionCount +
                ", rowKey='" + rowKey + '\'' +
                '}';
    }
}