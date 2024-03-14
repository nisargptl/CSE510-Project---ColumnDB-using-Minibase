package global;

import java.io.Serializable;

public class TID implements Serializable {
    public int numRIDs;
    public int position;
    public RID[] recordIDs;
    public TID(){}
    // Default constructor for class
    public TID(int numRIDs) {
        this.numRIDs = numRIDs;
    }

    // Constructor of class
    public TID(int numRIDs, int position) {
        this.position = position;
        this.numRIDs = numRIDs;
    }

    // Constructor of class
    public TID(int numRIDs, int position, RID[] recordIDs) {
        this.numRIDs = numRIDs;
        this.position = position;
        this.recordIDs = recordIDs;
    }

    //    void copyTid(TID tid)
    //    make a copy of the given tid
    public void copyTid(TID tid) {
        numRIDs = tid.numRIDs;
        position = tid.position;
        recordIDs = tid.recordIDs;
    }

    //    boolean equals(TID tid)
    //    Compares two TID objects
    public boolean equals(TID tid) {

        return this.numRIDs == tid.numRIDs && this.position == tid.position && this.recordIDs == tid.recordIDs;
    }

    //    void writeToByteArray(byte[] array, int offset)
    //    Write the tid into a byte array at offset
    public void writeToByteArray(byte[] array, int offset)
        throws java.io.IOException {
            Convert.setIntValue (numRIDs, offset, array);
            Convert.setIntValue(position, offset + 4, array);
        }

    //    void setPosition(int position)
    //    set the position attribute with the given value
    public void setPosition(int position) {
        this.position = position;
    }

    //    void setRID(int column, RID recordID)
    //    set the RID of the given column
    public void setRID(int column, RID recordID) {
        this.recordIDs[column] = recordID;
    }

    public int getPosition() {
        return position;
    }
}