package cloudera.se.fraud.demo.model;

public class StorePOJO {

    String name;

    String location;

    String address;

    String phone;

    String merchantType;

    String mcc;

    int merchantId;

    public StorePOJO() {
    }

    public StorePOJO(int merchantId, String name, String address, String location, String phone, String merchantType, String mcc) {
        this.merchantId = merchantId;
        this.name=name;
        this.address=address;
        this.location=location;
        this.phone=phone;
        this.merchantType=merchantType;
        this.mcc=mcc;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String value) {
        this.name = value;
    }

    public String getLocation() {
        return this.location;
    }

    public void setLocation(String value) {
        this.location = value;
    }

    public String getAddress() {
        return this.address;
    }

    public void setAddress(String value) {
        this.address = value;
    }

    public String getPhone() {
        return this.phone;
    }

    public void setPhone(String value) {
        this.phone = value;
    }

    public String getMerchantType() {
        return this.merchantType;
    }

    public void setMerchantType(String value) {
        this.merchantType = value;
    }

    public String getMcc() {
        return this.mcc;
    }

    public void setMcc(String value) {
        this.mcc = value;
    }

    public int getMerchantId() {
        return this.merchantId;
    }

    public void setMerchantId(int value) {
        this.merchantId = value;
    }
}