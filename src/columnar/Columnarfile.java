package columnar;

import btree.BTreeFile;
import btree.KeyFactory;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.*;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class Columnarfile {
    short numColumns;
    // field to store all the types of the columns
    AttrType[] atype = null;
    // Stores sizes of the Attributes
    short[] attrsizes;

    //Best way handle +2 bytes for strings instead of multiple ifs
    short[] asize;
    // The column files for the c-store
    private Heapfile[] hf = null;
    String fname = null;
    //int tupleCnt = 0;
    Tuple hdr = null;
    RID hdrRid = null;
    // Maps Attributes to position
    HashMap<String, Integer> columnMap;

    public Columnarfile(String _fileName) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile f;
        Scan scan;
        fname = _fileName;
        columnMap = new HashMap<>();

        try {
            // Fetching the headerfile of the columnarfile to check if the columnarfile actually exists on the disk
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(_fileName + ".hdr");

            if (pid == null) {
                throw new Exception("Columnar with the name: " + _fileName + ".hdr doesn't exists");
            }

            f = new Heapfile(_fileName + ".hdr");

            scan = f.openScan();
            hdrRid = new RID();

            // Fetching the header tuple from the disk
            Tuple hdr = scan.getNext(hdrRid);
            hdr.setHeaderMetaData();

            // Header file format - numColumns, AttrType[0], AttrSize[0], AttrType[1], AttrSize[1], AttrType[2], AttrSize[2],  .... AttrType[numColumns-1], AttrSize[numColumns-1]
            this.numColumns = (short) hdr.getIntFld(1);
            atype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize = new short[numColumns];

            int k = 0;
            for (int i = 0; i < numColumns; i++, k = k + 3) {
                atype[i] = new AttrType(hdr.getIntFld(2 + k));
                attrsizes[i] = (short) hdr.getIntFld(3 + k);
                String colName = hdr.getStrFld(4 + k);

                columnMap.put(colName, i);
                asize[i] = attrsizes[i];
                if (atype[i].attrType == AttrType.attrString)
                    asize[i] += 2;
            }
            hf = new Heapfile[numColumns];
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Columnarfile(String _fileName, int numcols, AttrType[] types, short[] attrSizes) throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        String[] colnames = new String[numcols];
        boolean status = true;
        Heapfile hdrFile = null;
        columnMap = new HashMap<>();
        try {
            hf = new Heapfile[numcols];
            //hf[0] for header file by default
            hdrFile = new Heapfile(_fileName + ".hdr");

        } catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        for(int i = 0; i < numcols; i++) {
            int cnum = i + 1;
            colnames[i] = _fileName + "." + cnum;
        }

        if (status == true) {
            numColumns = (short) (numcols);
            this.fname = _fileName;
            atype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize = new short[numColumns];
            int k = 0;
            for (int i = 0; i < numColumns; i++) {
                atype[i] = new AttrType(types[i].attrType);
                switch (types[i].attrType) {
                    case 0:
                        asize[i] = attrsizes[i] = attrSizes[k];
                        asize[i] += 2;
                        k++;
                        break;
                    case 1:
                    case 2:
                        asize[i] = attrsizes[i] = 4;
                        break;
                    case 3:
                        asize[i] = attrsizes[i] = 1;
                        break;
                    case 4:
                        attrsizes[i] = 0;
                        break;
                }
            }

            AttrType[] htypes = new AttrType[2 + (numColumns * 3)];
            htypes[0] = new AttrType(AttrType.attrInteger);
            for (int i = 1; i < htypes.length - 1; i = i + 3) {
                htypes[i] = new AttrType(AttrType.attrInteger);
                htypes[i + 1] = new AttrType(AttrType.attrInteger);
                htypes[i + 2] = new AttrType(AttrType.attrString);
            }
            htypes[htypes.length - 1] = new AttrType(AttrType.attrInteger);
            short[] hsizes = new short[numColumns];
            for (int i = 0; i < numColumns; i++) {
                hsizes[i] = 20; //column name can't be more than 20 chars
            }
            hdr = new Tuple();
            hdr.setHdr((short) htypes.length, htypes, hsizes);
            int size = hdr.size();

            hdr = new Tuple(size);
            hdr.setHdr((short) htypes.length, htypes, hsizes);
            hdr.setIntFld(1, numColumns);
            int j = 0;
            for (int i = 0; i < numColumns; i++, j = j + 3) {
                hdr.setIntFld(2 + j, atype[i].attrType);
                hdr.setIntFld(3 + j, attrsizes[i]);
                hdr.setStrFld(4 + j, colnames[i]);
                columnMap.put(colnames[i], i);
            }
            hdrRid = hdrFile.insertRecord(hdr.returnTupleByteArray());
        }
    }

    // Functionality - Deletes the columnar file by deleting the heapfiles of the columns and the heapfile of the header
    // Parameters - Nothing
    // Returns - Nothing
    public void deleteColumnarFile() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException, HFException {
        Heapfile hdr = new Heapfile(fname + "hdr");
        hdr.deleteFile();
        Heapfile idx = new Heapfile(fname + "idx");
        idx.deleteFile();

        for (int i = 0; i < numColumns; i++) {
            hf[i].deleteFile();
        }
        fname = null;
        numColumns = 0;
    }

    // Functionality - Inserts tuple into a columnar file
    // Parameters -
    // 1. byte[] data - The byte array of the data to be inserted into the columnarfile
    // Returns -
    // 1. the TID object of the inserted tuple
    public TID insertTuple(byte[] data) throws Exception {
        int offset = getOffset();
        RID[] rids = new RID[numColumns];
        int position = 0;

        for (int i = 0; i < numColumns; i++) {
            // Need to separate each rec from the byte array "data" to insert them into separate heapfiles
            byte[] rec = new byte[asize[i]];
            System.arraycopy(data, offset, rec, 0, asize[i]);

            // Inserting one cell as a separate record in the corresponding column heapfile
            rids[i] = getColumn(i).insertRecord(rec);

            // Updating the offset after insertion of the record
            offset += asize[i];
            if(i+1 == numColumns){
                position = getColumn(1).positionOfRecord(rids[1]);
            }
        }

        return new TID(numColumns, position, rids);
    }

    // Functionality - Gets the tuple given the TID of that tuple in the columnarfile
    // Parameters -
    // 1. TID tid - The TID of the tuple to be retrieved
    // Returns-
    // The tuple represented by the TID in the columnarfile
    public Tuple getTuple(TID tid) throws Exception {
        Tuple res = new Tuple(getTupleSize());
        res.setHdr(numColumns, atype, getStrSize());

        byte[] data = res.getTupleByteArray();
        int offset = getOffset();
        for (int i = 0; i < numColumns; i++) {
            Tuple t = getColumn(i).getRecord(tid.recordIDs[i]);
            System.arraycopy(t.getTupleByteArray(), 0, data, offset, asize[i]);
            offset += asize[i];
        }
        res.tupleInit(data, 0, data.length);
        return res;
    }

    // Functionality - Gets the number of tuples in the columnarfile
    // Parameters - None
    // Returns - an integer which is the number of tuples in the columnarfile
    public int getTupleCnt() throws HFDiskMgrException, HFException, HFBufMgrException, IOException, InvalidSlotNumberException, InvalidTupleSizeException {
        return getColumn(0).getRecCnt();
    }

    // Functionality - Opens a tuplescan for the columnarfile
    // Parameters - None
    // Returns - A tuplescan object.
    public TupleScan openTupleScan() throws Exception {
        return new TupleScan(this);
    }

    // Functionality - opens a tuple scan but only on the specified columns - exploits the advantage of columnarfiles (non need to scan all the columns which we have to do for row stores)
    // Parameters -
    // 1. short[] columnNums - The column numbers for which we have to open the scans
    // Returns - A tuplescan object.
    public TupleScan openTupleScan(short[] columnNums) throws Exception {
        return new TupleScan(this, columnNums);
    }

    // Functionality - opens a scan on one single column
    // Parameters -
    // 1. int colNum - The number of the column for which the scan object has to be returned
    // Returns - returns the scan object of the given column number
    public Scan openColumnScan(int colNum) throws Exception {
        if (colNum >= hf.length) {
            throw new Exception("Invalid Column number");
        }
        return new Scan(getColumn(colNum));
    }

    // Functionality - Updates the tuple of the given TID with the "updatedTup"
    // Parameters -
    // 1. TID tid - The tid of the tuple to be updated
    // 2. Tuple updatedTup - The data with the updated tuple
    // Returns - the boolean value false if failed true otherwise
    public boolean updateTuple(TID tid, Tuple updatedTup) {
        try {
            int offset = getOffset();
            byte[] tuplePtr = updatedTup.getTupleByteArray();
            for (int i = 0; i < numColumns; i++) {
                byte[] data = new byte[asize[i]];
                System.arraycopy(tuplePtr, offset, data, 0, asize[i]);
                Tuple t = new Tuple(asize[i]);
                t.tupleInit(data, 0, data.length);
                getColumn(i).updateRecord(tid.recordIDs[i], t);
                offset += asize[i];
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean updateColumnofTuple(TID tidarg, Tuple newtuple, int column) {
        try {
            int offset = getOffset(column);
            byte[] tuplePtr = newtuple.getTupleByteArray();
            Tuple t = new Tuple(asize[column]);
            byte[] data = t.getTupleByteArray();
            System.arraycopy(tuplePtr, offset, data, 0, asize[column]);
            t.tupleInit(data, 0, data.length);
            getColumn(column).updateRecord(tidarg.recordIDs[column], t);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean createBtreeIndex(int columnNo) throws Exception {
        String indexName = getBTName(columnNo);

        int keyType = atype[columnNo].attrType;
        int keySize = asize[columnNo];
        int deleteFashion = 0;
        BTreeFile bTreeFile = new BTreeFile(indexName, keyType, keySize, deleteFashion);
        Scan columnScan = openColumnScan(columnNo);
        RID rid = new RID();
        Tuple tuple;
        while (true) {
            tuple = columnScan.getNext(rid);
            if (tuple == null) {
                break;
            }
            int position = getColumn(columnNo).positionOfRecord(rid);
            bTreeFile.insert(KeyFactory.getKeyClass(tuple.getTupleByteArray(), atype[columnNo], asize[columnNo]), rid);
        }
        columnScan.closescan();
        addIndexToColumnar(0, indexName);
        return true;
    }

    public boolean purgeAllDeletedTuples() throws HFDiskMgrException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException, SortException {

        boolean status = OK;
        Sort deletedTuples = null;
        RID rid;
        Heapfile f = null;
        int pos_marked;
        boolean done = false;
        try {
            f = new Heapfile(getDeletedFileName());
        } catch (Exception e) {
            status = FAIL;
            System.err.println(" Could not open heapfile");
            e.printStackTrace();
        }

        if (status == OK) {
            try {
                AttrType[] types = new AttrType[1];
                types[0] = new AttrType(AttrType.attrInteger);
                short[] sizes = new short[0];
                FldSpec[] projlist = new FldSpec[1];
                projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
                FileScan fs = new FileScan(getDeletedFileName(), types, sizes, (short) 1, 1, projlist, null);
                deletedTuples = new Sort(types, (short) 1, sizes, fs, 1, new TupleOrder(TupleOrder.Descending), 4, 10);

            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {
            int i = 0;
            Tuple tuple;
            while (!done) {
                try {
                    rid = new RID();
                    tuple = deletedTuples.get_next();
                    if (tuple == null) {
                        deletedTuples.close();
                        done = true;
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(deletedTuples != null)
                        deletedTuples.close();
                    f.deleteFile();
                    return false;
                }
            }
        }
        f.deleteFile();

        return true;
    }

    private boolean addIndexToColumnar(int indexType, String indexName) {

        try {
            AttrType[] itypes = new AttrType[2];
            itypes[0] = new AttrType(AttrType.attrInteger);
            itypes[1] = new AttrType(AttrType.attrString);
            short[] isizes = new short[1];
            isizes[0] = 40; //index name can't be more than 40 chars
            Tuple t = new Tuple();
            t.setHdr((short) 2, itypes, isizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short) 2, itypes, isizes);
            t.setIntFld(1, indexType);
            t.setStrFld(2, indexName);
            Heapfile f = new Heapfile(fname + ".idx");
            f.insertRecord(t.getTupleByteArray());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public Heapfile getColumn(int columnNo) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        if (hf[columnNo] == null)
            hf[columnNo] = new Heapfile(fname + columnNo);
        return hf[columnNo];
    }

    public void close() {
        if (hf != null) {
            for (int i = 0; i < hf.length; i++)
                hf[i] = null;
        }
    }

    public int getTupleSize() {
        int size = getOffset();
        for (int i = 0; i < numColumns; i++) {
            size += asize[i];
        }
        return size;
    }

    public short[] getStrSize() {
        int n = 0;
        for (int i = 0; i < numColumns; i++) {
            if (atype[i].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for (int i = 0; i < numColumns; i++) {
            if (atype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = attrsizes[i];
            }
        }

        return strSize;
    }

    public short[] getStrSize(short[] targetColumns) {
        int n = 0;
        for (int i = 0; i < targetColumns.length; i++) {
            if (atype[targetColumns[i]].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for (int i = 0; i < targetColumns.length; i++) {
            if (atype[targetColumns[i]].attrType == AttrType.attrString) {
                strSize[cnt++] = attrsizes[targetColumns[i]];
            }
        }

        return strSize;
    }

    public int getOffset() {
        // 4 = int for num of cols
        return 4 + (numColumns * 2);
    }

    public int getOffset(int column) {
        int offset = 4 + (numColumns * 2);
        for (int i = 0; i < column; i++) {
            offset += asize[i];
        }
        return offset;
    }

    public String getColumnarFileName() {
        return fname;
    }


    public AttrType[] getAttributes() {
        return atype;
    }

    public int getAttributePosition(String name) {
        name = fname + "." + name;
        System.out.println(name);
        System.out.println(columnMap.get(name));
        return columnMap.get(name);
    }

    public String getBTName(int columnNo) {
        return "BT" + "." + fname + "." + columnNo;
    }

    /**
     * return the BitMap file name by following the conventions
     *
     * @param columnNo
     * @param value
     * @return
     */
    public String getBMName(int columnNo, ValueClass value) {
        return "BM" + "." + fname + "." + columnNo + "." + value.toString();
    }

    public String getDeletedFileName() {
        return fname + ".del";
    }
    /**
     * given a column returns the AttrType
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public AttrType getAttrtypeforcolumn(int columnNo) throws Exception {
        if (columnNo < numColumns) {
            return atype[columnNo];
        } else {
            throw new Exception("Invalid Column Number");
        }
    }

    /**
     *  given the column returns the size of AttrString
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public short getAttrsizeforcolumn(int columnNo) throws Exception {
        if (columnNo < numColumns) {
            return attrsizes[columnNo];
        } else {
            throw new Exception("Invalid Column Number");
        }
    }

    public Tuple getTuple(int position) throws
            Exception {

        for(int i=0; i < hf.length; i++) {
            hf[i] = new Heapfile(getColumnarFileName() + i);
        }
        Tuple JTuple = new Tuple();
        // set the header which attribute types of the targeted columns
        JTuple.setHdr((short) hf.length, atype, getStrSize());

        JTuple = new Tuple(JTuple.size());
        JTuple.setHdr((short) hf.length, atype, getStrSize());
        for (int i = 0; i < hf.length; i++) {
            RID rid = hf[i].recordAtPosition(position);
            Tuple record = hf[i].getRecord(rid);
            switch (atype[i].attrType) {
                case AttrType.attrInteger:
                    // Assumed that col heap page will have only one entry
                    JTuple.setIntFld(i + 1,
                            Convert.getIntValue(0, record.getTupleByteArray()));
                    break;
                case AttrType.attrString:
                    JTuple.setStrFld(i + 1,
                            Convert.getStrValue(0, record.getTupleByteArray(), attrsizes[i] + 2));
                    break;
                default:
                    throw new Exception("Attribute indexAttrType not supported");
            }
        }

        return JTuple;
    }
}
