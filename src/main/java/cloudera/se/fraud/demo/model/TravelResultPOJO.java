package cloudera.se.fraud.demo.model;

public class TravelResultPOJO {

    int score;

    double distance;

    long elapsedSec;

    public TravelResultPOJO() {
    }

    public int getScore() {
        return this.score;
    }

    public void setScore(int value) {
        this.score = value;
    }

    public double getDistance() {
        return this.distance;
    }

    public void setDistance(double value) {
        this.distance = value;
    }

    public long getElapsedSec() {
        return this.elapsedSec;
    }

    public void setElapsedSec(long value) {
        this.elapsedSec = value;
    }
}
