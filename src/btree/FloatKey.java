package btree;

public class FloatKey extends KeyClass {

    private Float key;

    public FloatKey(Float value) {
        key = value;
    }

    public String toString() {
        return key.toString();
    }
    public Float getKey() {
        return key;
    }

    public void setKey(Float value) {
        key = value;
    }
}