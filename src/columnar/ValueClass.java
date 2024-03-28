package columnar;

public abstract class ValueClass<T> {
    protected T value;

    public ValueClass(T val) {
        this.value = val;
    }

    public T getValue() {
        return this.value;
    }

    @Override
    public java.lang.String toString() {
        return java.lang.String.valueOf(value);
    }
}

class ValueInt extends ValueClass<Integer> {
    public ValueInt(Integer val) {
        super(val);
    }
}

class ValueFloat extends ValueClass<Float> {
    public ValueFloat(Float val) {
        super(val);
    }
}

class ValueString extends ValueClass<java.lang.String> {
    public ValueString(java.lang.String val) {
        super(val);
    }
}
