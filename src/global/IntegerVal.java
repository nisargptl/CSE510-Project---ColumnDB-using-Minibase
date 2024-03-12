package global;

import java.nio.ByteBuffer;

public class IntegerVal extends ValueClass {
    int value;
    public IntegerVal(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.value = buffer.getInt();
    }

    public int getValue() {
        return this.value;
    }
}
