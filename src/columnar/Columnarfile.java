package columnar;

import bitmap.BitMapFile;
import btree.IntegerKey;
import btree.KeyDataEntry;
import heap.*;
import global.*;
import btree.KeyFactory;
import iterator.*;
import btree.BTreeFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class Columnarfile {
    String fname;
    boolean _fileDeleted = false;
    short numColumns;
    AttrType[] _ctype;
    short[] attrsizes;
    short[] asize;
    private Heapfile[] _columnHeaps;
    Heapfile _deletedTuples;
    Tuple _hdr;
    RID _hdrRid;
    HashMap<String, Integer> columnMap;
    HashMap<String, BitMapFile> BMMap = new HashMap<>();


    public Columnarfile(String _fileName) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile hf;
        Scan scan;
        fname = _fileName;
        columnMap = new HashMap<>();

        try {
            // Fetching the headerfile of the columnarfile to check if the columnarfile
            // actually exists on the disk
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(_fileName + ".hdr");

            if (pid == null) {
                throw new Exception(
                        "Columnar file " + _fileName + "does not exist! Please try again with a different CF name");
            }

            hf = new Heapfile(_fileName + ".hdr");

            scan = hf.openScan();
            _hdrRid = new RID();

            // Fetching the header tuple from the disk
            Tuple _hdr = scan.getNext(_hdrRid);
            _hdr.setHeaderMetaData();

            // Header file format - numColumns, AttrType[0], AttrSize[0], AttrType[1],
            // AttrSize[1], AttrType[2], AttrSize[2], .... AttrType[numColumns-1],
            // AttrSize[numColumns-1]
            this.numColumns = (short) _hdr.getIntFld(1);
            _ctype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize = new short[numColumns];

            int k = 0;
            for (int i = 0; i < numColumns; i++, k = k + 3) {
                _ctype[i] = new AttrType(_hdr.getIntFld(2 + k));
                attrsizes[i] = (short) _hdr.getIntFld(3 + k);
                String colName = _hdr.getStrFld(4 + k);

                columnMap.put(colName, i);
                asize[i] = attrsizes[i];
                if (_ctype[i].attrType == AttrType.attrString)
                    asize[i] += 2;
            }
            _columnHeaps = new Heapfile[numColumns];
            scan.closescan();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Columnarfile(String _fileName, int n, AttrType[] types, short[] attrSizes)
            throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException,
            SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        String[] colnames = new String[n];
        boolean status = true;
        Heapfile _hdrFile = null;
        columnMap = new HashMap<>();
        try {
            _columnHeaps = new Heapfile[n];

            _hdrFile = new Heapfile(_fileName + ".hdr");

        } catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        for (int i = 0; i < n; i++) {
            int cnum = i + 1;
            colnames[i] = _fileName + "." + cnum;
        }

        if (status) {
            numColumns = (short) (n);
            this.fname = _fileName;
            _ctype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize = new short[numColumns];
            int k = 0;
            for (int i = 0; i < numColumns; i++) {
                _ctype[i] = new AttrType(types[i].attrType);
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
                hsizes[i] = 20; // column name can't be more than 20 chars
            }
            _hdr = new Tuple();
            _hdr.setHdr((short) htypes.length, htypes, hsizes);
            int size = _hdr.size();

            _hdr = new Tuple(size);
            _hdr.setHdr((short) htypes.length, htypes, hsizes);
            _hdr.setIntFld(1, numColumns);
            int j = 0;
            for (int i = 0; i < numColumns; i++, j = j + 3) {
                _hdr.setIntFld(2 + j, _ctype[i].attrType);
                _hdr.setIntFld(3 + j, attrsizes[i]);
                _hdr.setStrFld(4 + j, colnames[i]);
                columnMap.put(colnames[i], i);
            }
            _hdrRid = _hdrFile.insertRecord(_hdr.returnTupleByteArray());
        }
    }

    // Functionality - Deletes the columnar file by deleting the heapfiles of the
    // columns and the heapfile of the header
    // Parameters - Nothing
    // Returns - Nothing
    public void deleteColumnarFile() throws InvalidSlotNumberException, FileAlreadyDeletedException,
            InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException, HFException {
        Heapfile _hdr = new Heapfile(fname + "hdr");
        _hdr.deleteFile();
        Heapfile idx = new Heapfile(fname + "idx");
        idx.deleteFile();

        for (int i = 0; i < numColumns; i++) {
            _columnHeaps[i].deleteFile();
        }
        fname = null;
        _fileDeleted = true;
        numColumns = 0;
    }

    // Functionality - Inserts tuple into a columnar file
    // Parameters -
    // 1. byte[] data - The byte array of the data to be inserted into the
    // columnarfile
    // Returns -
    // 1. the TID object of the inserted tuple
    public TID insertTuple(byte[] data) throws Exception {
        int offset = getOffset();
        RID[] rids = new RID[numColumns];
        int position = 0;

        for (int i = 0; i < numColumns; i++) {
            // Need to separate each rec from the byte array "data" to insert them into
            // separate heapfiles
            byte[] rec = new byte[asize[i]];
            System.arraycopy(data, offset, rec, 0, asize[i]);

            // Inserting one cell as a separate record in the corresponding column heapfile
            rids[i] = getColumn(i).insertRecord(rec);

            // Updating the offset after insertion of the record
            offset += asize[i];
            if (i + 1 == numColumns) {
                position = getColumn(1).positionOfRecord(rids[1]);
            }
        }

        return new TID(numColumns, position, rids);
    }

    // Functionality - Gets the tuple given the TID of that tuple in the
    // columnarfile
    // Parameters -
    // 1. TID tid - The TID of the tuple to be retrieved
    // Returns-
    // The tuple represented by the TID in the columnarfile
    public Tuple getTuple(TID tid) throws Exception {
        Tuple res = new Tuple(getTupleSize());
        res.setHdr(numColumns, _ctype, getStrSize());

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
    public int getTupleCnt() throws HFDiskMgrException, HFException, HFBufMgrException, IOException,
            InvalidSlotNumberException, InvalidTupleSizeException {
        return getColumn(0).getRecCnt();
    }

    // Functionality - Opens a tuplescan for the columnarfile
    // Parameters - None
    // Returns - A tuplescan object.
    public TupleScan openTupleScan() throws Exception {
        return new TupleScan(this);
    }

    // Functionality - opens a tuple scan but only on the specified columns -
    // exploits the advantage of columnarfiles (non need to scan all the columns
    // which we have to do for row stores)
    // Parameters -
    // 1. short[] columnNums - The column numbers for which we have to open the
    // scans
    // Returns - A tuplescan object.
    public TupleScan openTupleScan(short[] columnNums) throws Exception {
        return new TupleScan(this, columnNums);
    }

    // Functionality - opens a scan on one single column
    // Parameters -
    // 1. int colNum - The number of the column for which the scan object has to be
    // returned
    // Returns - returns the scan object of the given column number
    public Scan openColumnScan(int colNum) throws Exception {
        if (colNum >= _columnHeaps.length) {
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
        String indexName = getBTName(columnNo - 1); // todo: changed this
        // String indexName = getBTName(columnNo + 1);

        int keyType = _ctype[columnNo - 1].attrType;
        int keySize = asize[columnNo - 1];
        int deleteFashion = 0;
        BTreeFile bTreeFile = new BTreeFile(indexName, keyType, keySize, deleteFashion);
        Scan columnScan = openColumnScan(columnNo - 1);
        RID rid = new RID();
        Tuple tuple;
        while (true) {
            tuple = columnScan.getNext(rid);
            if (tuple == null) {
                break;
            }

            bTreeFile.insert(
                    KeyFactory.getKeyClass(tuple.getTupleByteArray(), _ctype[columnNo - 1], asize[columnNo - 1]), rid);
        }
        columnScan.closescan();
        bTreeFile.close();
        return true;
    }

    public String getBMName(int columnNo, AttrType attrType) {
        return "BM" + "." + fname + "." + columnNo + "." + attrType.toString();
    }

    public String[] getAvailableBM(int columnNo) {
        List<String> bmName = new ArrayList<>();
        AttrType attrType = _ctype[columnNo];

        String prefix = getBMName(columnNo, attrType);

        for(String s : BMMap.keySet()){
            if(s.substring(0,prefix.length()).equals(prefix)){
                bmName.add(s);
            }
        }
        return  bmName.toArray(new String[bmName.size()]);
    }

//    public boolean createBitMapIndex(int columnNo, ValueClass value) throws Exception {
//        // Define the name for the bitmap file based on the column number and the
//        // specific value
//        String indexName = getBMName(columnNo, value);
//
//        String prefix = getBMName(columnNo, attrType);
//
//        for(String s : BMMap.keySet()){
//            if(s.substring(0,prefix.length()).equals(prefix)){
//                bmName.add(s);
//            }
//
//            // Directly extract and compare the value from the tuple based on the column's
//            // attribute type
//            boolean matchesValue = false;
//            switch (_ctype[columnNo].attrType) {
//                case AttrType.attrInteger:
//                    int intValue = tuple.getIntFld(1);
//                    if (value instanceof ValueInt && ((ValueInt) value).getValue() == intValue) {
//                        matchesValue = true;
//                    }
//                    break;
//                case AttrType.attrString:
//                    String stringValue = tuple.getStrFld(1);
//                    if (value instanceof ValueString && ((ValueString) value).getValue().equals(stringValue)) {
//                        matchesValue = true;
//                    }
//                    break;
//                case AttrType.attrReal:
//                    float floatValue = tuple.getFloFld(1);
//                    if (value instanceof ValueFloat && ((ValueFloat) value).getValue() == floatValue) {
//                        matchesValue = true;
//                    }
//                    break;
//                // Include other types as necessary
//            }
//
//            if (matchesValue) {
//                bitMapFile.insert(position);
//            } else {
//                bitMapFile.delete(position);
//            }
//            position++;
//        }
//        return  bmName.toArray(new String[bmName.size()]);
//    }

    public boolean createAllBitMapIndexForColumn(int columnNo) throws Exception {
        // Initialize a map to keep track of bitmap files created during this operation
        HashMap<String, BitMapFile> BMMap = new HashMap<>();

        // Open a scan on the specified column
        Scan columnScan = openColumnScan(columnNo - 1);
        RID rid = new RID();
        Tuple tuple;
        int position = 0;

        while (true) {
            tuple = columnScan.getNext(rid);
            if (tuple == null) {
                break;
            }

            // Use _ctype[columnNo] directly to identify the attribute type
            AttrType attrType = _ctype[columnNo - 1];

            // Generate a bitmap file name based on the column number and the attribute type
            String bitMapFileName = getBMName(columnNo, attrType);

            // Create or retrieve a BitMapFile for the current attribute type
            BitMapFile bitMapFile = BMMap.computeIfAbsent(bitMapFileName, k -> {
                try {
                    BitMapFile newBitMapFile = new BitMapFile(bitMapFileName, this, columnNo, attrType);
                    addIndexToColumnar(1, bitMapFileName); // Register the new bitmap index
                    return newBitMapFile;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            });

            // Insert the position into the bitmap file
            if (bitMapFile != null) {
                bitMapFile.insert(position);
            }

            position++;
        }

        columnScan.closescan();

        // Close all the bitmap files
        for (BitMapFile bitMapFile : BMMap.values()) {
            if (bitMapFile != null) {
                bitMapFile.close();
            }
        }

        return true;
    }

    public boolean markTupleDeleted(int position) {
        String name = getDeletedFileName();
        try {
            Heapfile f = new Heapfile(name);
            Integer pos = position;
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrInteger);
            short[] sizes = new short[0];
            Tuple t = new Tuple(10);
            t.setHdr((short) 1, types, sizes);
            t.setIntFld(1, pos);
            f.insertRecord(t.getTupleByteArray());

//            for (int i = 0; i < numColumns; i++) {
//                Tuple tuple = getColumn(i).getRecord(position);
//                ValueClass valueClass;
//                KeyClass keyClass;
//                valueClass = ValueFactory.getValueClass(tuple.getTupleByteArray(),
//                        atype[i],
//                        asize[i]);
//                keyClass = KeyFactory.getKeyClass(tuple.getTupleByteArray(),
//                        atype[i],
//                        asize[i]);
//
//                String bTreeFileName = getBTName(i);
//                String bitMapFileName = getBMName(i, valueClass);
//                if (BTMap.containsKey(bTreeFileName)) {
//                    BTreeFile bTreeFile = getBTIndex(bTreeFileName);
//                    bTreeFile.Delete(keyClass, position);
//                }
//                if (BMMap.containsKey(bitMapFileName)) {
//                    BitMapFile bitMapFile = getBMIndex(bitMapFileName);
//                    bitMapFile.delete(position);
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * @param tidarg
     * @return
     */
    public boolean markTupleDeleted(TID tidarg) {
        return markTupleDeleted(tidarg.position);
    }

    /**
     * Purges all tuples marked for deletion. Removes keys/positions from indexes too
     *
     * @return
     * @throws HFDiskMgrException
     * @throws InvalidTupleSizeException
     * @throws IOException
     * @throws InvalidSlotNumberException
     * @throws FileAlreadyDeletedException
     * @throws HFBufMgrException
     * @throws SortException
     */
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
                    pos_marked = Convert.getIntValue(6, tuple.getTupleByteArray());
                    for (int j = 0; j < numColumns; j++) {
                        rid = getColumn(j).recordAtPosition(pos_marked);
                        getColumn(j).deleteRecord(rid);

//                        for (String fileName : BMMap.keySet()) {
//                            int columnNo = Integer.parseInt(fileName.split("\\.")[2]);
//                            if (columnNo == i) {
//                                BitMapFile bitMapFile = getBMIndex(fileName);
//                                bitMapFile.fullDelete(pos_marked);
//                            }
//                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    if (deletedTuples != null)
                        deletedTuples.close();
                    f.deleteFile();
                    return false;
                }
            }
        }
        f.deleteFile();

        return true;
    }

    public static byte[] objectToByteArray(Serializable object) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {

            // Serialize the object to the byte array
            oos.writeObject(object);

            // Get the byte array
            return bos.toByteArray();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private boolean addIndexToColumnar(int indexType, String indexName) {

        try {
            AttrType[] itypes = new AttrType[2];
            itypes[0] = new AttrType(AttrType.attrInteger);
            itypes[1] = new AttrType(AttrType.attrString);
            short[] isizes = new short[1];
            isizes[0] = 40; // index name can't be more than 40 chars
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
        if (_columnHeaps[columnNo] == null)
            _columnHeaps[columnNo] = new Heapfile(fname + columnNo);
        return _columnHeaps[columnNo];
    }

    public void close() {
        if (_columnHeaps != null) {
            for (int i = 0; i < _columnHeaps.length; i++)
                _columnHeaps[i] = null;
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
            if (_ctype[i].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for (int i = 0; i < numColumns; i++) {
            if (_ctype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = attrsizes[i];
            }
        }

        return strSize;
    }

    public short[] getStrSize(short[] targetColumns) {
        int n = 0;
        for (int i = 0; i < targetColumns.length; i++) {
            if (_ctype[targetColumns[i]].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for (int i = 0; i < targetColumns.length; i++) {
            if (_ctype[targetColumns[i]].attrType == AttrType.attrString) {
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
        return _ctype;
    }

    public int getAttributePosition(String name) {
        name = fname + "." + name;
        System.out.println(name);
        System.out.println(columnMap.get(name));
        return columnMap.get(name);
    }

    public String getBTName(int columnNo) {
        columnNo += 1;
        return "BT" + "." + fname + "." + columnNo;
    }

    public String getDeletedFileName() {
        return fname + ".del";
    }

    public AttrType getAttrtypeforcolumn(int columnNo) throws Exception {
        if (columnNo < numColumns) {
            return _ctype[columnNo];
        } else {
            throw new Exception("Invalid Column Number");
        }
    }

    public short getAttrsizeforcolumn(int columnNo) throws Exception {
        if (columnNo < numColumns) {
            return attrsizes[columnNo];
        } else {
            throw new Exception("Invalid Column Number");
        }
    }
}
