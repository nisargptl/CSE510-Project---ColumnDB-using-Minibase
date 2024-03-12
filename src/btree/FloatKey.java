package btree;

public class FloatKey extends KeyClass {

    private Float key;

    public FloatKey(Float value) {
        key = new Float(value);
    }

    public String toString() {
        return key.toString();
    }
    public Float getKey() {
        return new Float(key);
    }

    public void setKey(Float value) {
        key = value;
    }
}