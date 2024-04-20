package columnar;

import bitmap.BitMapFile;
import btree.IntegerKey;
import btree.KeyDataEntry;
import heap.*;
import global.*;
import btree.KeyFactory;
import iterator.*;
import btree.BTreeFile;

import java.io.*;
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
    Heapfile _SortedDTuples;
    Tuple _hdr;
    RID _hdrRid;
    HashMap<String, Integer> columnMap;
    HashMap<String, BitMapFile> BMMap = new HashMap<>();
    HashMap<String, BTreeFile> BTMap;
    List<String> bmName = new ArrayList<>();

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

    // BITMAP CREATION IMPLEMENTATION
    public boolean createAllBitMapIndexForColumn(int columnNo) throws Exception {
        // Adjusted method to use class-level BMMap
        Scan columnScan = openColumnScan(columnNo - 1);
        RID rid = new RID();
        Tuple tuple;
        int position = 0;

        while (true) {
            tuple = columnScan.getNext(rid);
            if (tuple == null) {
                break;
            }

            // Retrieve and convert the value from the tuple
            AttrType attrType = _ctype[columnNo - 1];
            Object value = extractValueFromTuple(tuple, attrType); // You'll need to implement this

            // Use the value to determine the bitmap file name
            String bitMapFileName = getBMName(columnNo, attrType, value); // Modify to include value
            System.out.println("BITMAP FILE NAME: " + bitMapFileName);

            // Ensure a bitmap file for each unique value
            BitMapFile bitMapFile = BMMap.computeIfAbsent(bitMapFileName, k -> {
                try {
                    return new BitMapFile(bitMapFileName, this, columnNo, attrType);
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

        // Close all opened bitmap files
        for (BitMapFile bmp : BMMap.values()) {
            if (bmp != null)
                bmp.close();
        }

        return true;
    }

    public String getBMName(int columnNo, AttrType attrType, Object value) {
        // Convert the value to a string safe for inclusion in a filename.
        String valueString = convertValueToString(value);
        String filename = "BM" + "." + fname + "." + columnNo + "." + attrType.toString() + "." + valueString;
        return filename;
    }

    private String convertValueToString(Object value) {
        if (value == null) {
            return "null";
        }
        // Convert the value to a string, replacing spaces and special characters that
        // are not safe in filenames
        String valueString = value.toString().replaceAll("[^a-zA-Z0-9_]", "_");
        return valueString;
    }

    private Object extractValueFromTuple(Tuple tuple, AttrType attrType) throws IOException {
        // Assumes that the tuple consists of a single value of the type specified
        switch (attrType.attrType) {
            case AttrType.attrInteger:
                return Convert.getIntValue(0, tuple.getTupleByteArray());
            case AttrType.attrString:
                return Convert.getStrValue(0, tuple.getTupleByteArray(), tuple.getLength());
            // Add more cases as necessary for other data types
        }
        return null;
    }

    // todo
    public String[] getAvailableBM(int columnNo) throws Exception {
        // System.out.println("BMMAP SIZE: " + BMMap.size());
        Tuple tuple = new Tuple();
        AttrType attrType = _ctype[columnNo - 1];
        Object value = extractValueFromTuple(tuple, attrType);
        String bitMapFileName = getBMName(columnNo, attrType, value);

        bmName.add(bitMapFileName);
        return bmName.toArray(new String[bmName.size()]);
    }

    public boolean markTupleDeleted(int position) {
        String DeletedFileName = getDeletedFileName();
        try {
            Heapfile f = new Heapfile(DeletedFileName);
            Integer pos = position;
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrInteger);
            short[] sizes = new short[0];
            Tuple t = new Tuple(10);
            t.setHdr((short) 1, types, sizes);
            t.setIntFld(1, pos);
            f.insertRecord(t.getTupleByteArray());

            // for (int i = 0; i < numColumns; i++) {
            // Tuple tuple = getColumn(i).getRecord(position);
            // ValueClass valueClass;
            // KeyClass keyClass;
            // valueClass = ValueFactory.getValueClass(tuple.getTupleByteArray(),
            // atype[i],
            // asize[i]);
            // keyClass = KeyFactory.getKeyClass(tuple.getTupleByteArray(),
            // atype[i],
            // asize[i]);
            //
            // String bTreeFileName = getBTName(i);
            // String bitMapFileName = getBMName(i, valueClass);
            // if (BTMap.containsKey(bTreeFileName)) {
            // BTreeFile bTreeFile = getBTIndex(bTreeFileName);
            // bTreeFile.Delete(keyClass, position);
            // }
            // if (BMMap.containsKey(bitMapFileName)) {
            // BitMapFile bitMapFile = getBMIndex(bitMapFileName);
            // bitMapFile.delete(position);
            // }
            // }
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
    public boolean markTupleDeleted(TID tid) {
        return markTupleDeleted(tid.position);
    }

    /**
     * Purges all tuples marked for deletion. Removes keys/positions from indexes
     * too
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
    public boolean purgeAllDeletedTuples() throws HFDiskMgrException, InvalidTupleSizeException, IOException,
            InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException, SortException {

        Sort SortedDTuples = null;
        RID rid;
        Heapfile f = null;
        int position;
        boolean flag = false;
        try {
            f = new Heapfile(getDeletedFileName());
        } catch (Exception e) {
            System.err.println(" Could not open heapfile");
            e.printStackTrace();
            f.deleteFile();
            return false;
        }

        try {
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrInteger);
            short[] sizes = new short[0];
            FldSpec[] projlist = new FldSpec[1];
            projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
            FileScan fs = new FileScan(getDeletedFileName(), types, sizes, (short) 1, 1, projlist, null);
            SortedDTuples = new Sort(types, (short) 1, sizes, fs, 1, new TupleOrder(TupleOrder.Descending), 4, 10);

        } catch (Exception e) {
            System.err.println("*** Error opening scan\n");
            e.printStackTrace();
        }

        int i = 0;
        Tuple tuple;
        while (!flag) {
            try {
                rid = new RID();
                tuple = SortedDTuples.get_next();
                if (tuple == null) {
                    SortedDTuples.close();
                    flag = true;
                    break;
                }
                position = Convert.getIntValue(6, tuple.getTupleByteArray());
                for (int j = 0; j < numColumns; j++) {
                    rid = getColumn(j).recordAtPosition(position);
                    getColumn(j).deleteRecord(rid);

                    // for (String fileName : BMMap.keySet()) {
                    // int columnNo = Integer.parseInt(fileName.split("\\.")[2]);
                    // if (columnNo == i) {
                    // BitMapFile bitMapFile = getBMIndex(fileName);
                    // bitMapFile.fullDelete(position);
                    // }
                    // }
                }

            } catch (Exception e) {
                e.printStackTrace();
                if (SortedDTuples != null)
                    SortedDTuples.close();
                f.deleteFile();
                return false;
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

    // public int getPositionFromRID(Heapfile heap, RID rid) throws Exception {
    // Scan scan = heap.openScan();
    // RID currentRID;
    // Tuple tuple;
    // int position = 0; // Start position index from 0

    // while ((tuple = scan.getNext(currentRID = new RID())) != null) {
    // System.out.println("Current RID: " + currentRID);
    // if (currentRID.equals(rid)) {
    // scan.closescan();
    // return position;
    // }
    // position++;
    // }
    // scan.closescan();

    // throw new Exception("Given RID not found in the specified Heapfile.");
    // }

    // public TID getTIDFromRID(int columnNo, RID rid) throws Exception {
    // if (columnNo < 0 || columnNo >= numColumns) {
    // throw new IllegalArgumentException("Invalid column number.");
    // }

    // Heapfile column = getColumn(columnNo);
    // Tuple tuple = column.getRecord(rid); // Get the tuple from the column
    // int position = getPositionFromRID(column, rid); // Get the position of the
    // RID in its column

    // // Assuming RIDs in all columns for a given row are aligned
    // RID[] rids = new RID[numColumns];
    // for (int i = 0; i < numColumns; i++) {
    // if (i == columnNo) {
    // rids[i] = rid; // Assign the input RID directly for the queried column
    // } else {
    // Heapfile otherColumn = getColumn(i);
    // rids[i] = otherColumn.recordAtPosition(position); // or a similar method to
    // retrieve RID by position
    // }
    // }

    // return new TID(numColumns, position, rids);
    // }
}
