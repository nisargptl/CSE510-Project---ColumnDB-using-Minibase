package global;

public class StringVal extends ValueClass {
    String value;
    public StringVal(byte[] data) {
        this.value = new String(data);
    }

    public String getValue() {
        return this.value;
    }
}
