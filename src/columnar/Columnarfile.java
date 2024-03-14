package columnar;

import btree.*;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.HashMap;

import static global.GlobalConst.INVALID_PAGE;

interface Filetype {
  int TEMP = 0;
  int ORDINARY = 1;
}

public class Columnarfile implements Filetype {
  private int _tempfilecount = 0;
  PageId _firstDirPageId;
  private boolean _fileDeleted;
  private String _fileName;

  int numColumns;
  // One heapfile for each column
  Heapfile[] _columnHeaps;
  // Header file of the column page
  Heapfile _hdrFile;
  // RID of header file
  RID _hdrRid;
  // TIDs of Tuples marked for deletion but not acutally deleted from the database
  TID[] _deletedTuples;
  // Type information of each of the columns
  AttrType[] _ctypes;
  //store the attribute sizes of each column
  short[] _asizes;
  // Length of a tuple in the column store
  short[] asize;
  Tuple hdr;
  int tupLen;
  HashMap<String, BTreeFile> BTMap;
  HashMap<String, BTreeFile> BMMap;


public Columnarfile(java.lang.String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile f = null;
        Scan scan = null;
        RID rid = null;
        _fileName = name;
//        columnMap = new HashMap<>();
        try {
            // get the columnar header page
            System.out.println("FIRST");
            System.out.println(name);
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name + ".hdr");
            System.out.println("SECOND");
            if (pid == null) {
                throw new Exception("Columnar with the name: " + name + ".hdr doesn't exists");
            }

            f = new Heapfile(name + ".hdr");
            System.out.println("SECOND");
            //Header tuple is organized this way
            //NumColumns, AttrType1, AttrSize1, AttrName1, AttrType2, AttrSize2, AttrName3...

            scan = f.openScan();
            _hdrRid = new RID();
            Tuple hdr = scan.getNext(_hdrRid);
            hdr.setHeaderMetaData();
            this.numColumns = (short) hdr.getIntFld(1);
            _ctypes = new AttrType[numColumns];
            _asizes = new short[numColumns];
            asize = new short[numColumns];
            _columnHeaps = new Heapfile[numColumns];
            int k = 0;
            for (int i = 0; i < numColumns; i++, k = k + 3) {
                _ctypes[i] = new AttrType(hdr.getIntFld(2 + k));
                _asizes[i] = (short) hdr.getIntFld(3 + k);
                String colName = hdr.getStrFld(4 + k);
//                columnMap.put(colName, i);
                asize[i] = _asizes[i];
                if (_ctypes[i].attrType == AttrType.attrString)
                    asize[i] += 2;
            }

            System.out.println("LAST");
//            BTMap = new HashMap<>();
//            BMMap = new HashMap<>();

            // create a idx file to store all column which consists of indexes
//            pid = SystemDefs.JavabaseDB.get_file_entry(name + ".idx");
//            if (pid != null) {
//                f = new Heapfile(name + ".idx");
//                scan = f.openScan();
//                RID r = new RID();
//                Tuple t = scan.getNext(r);
//                while (t != null) {
//                    t.setHeaderMetaData();
//                    int indexType = t.getIntFld(1);
//                    if (indexType == 0)
//                        BTMap.put(t.getStrFld(2), null);
//                    else if (indexType == 1)
//                        BMMap.put(t.getStrFld(2), null);
//                    t = scan.getNext(r);
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



  public Columnarfile(String name, int numColumns, AttrType[] types, short[] attrSizes) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, Exception {
      RID rid1 = new RID();
      boolean status = true;
      Heapfile hdrFile = null;
//      columnMap = new HashMap<>();
      try {
          _columnHeaps = new Heapfile[numColumns];
          //hf[0] for header file by default
          hdrFile = new Heapfile(name + ".hdr");

      } catch (Exception e) {
          status = false;
          e.printStackTrace();
      }
      if (status == true) {
          numColumns = (short) (numColumns);
          this._fileName = name;
          _ctypes = new AttrType[numColumns];
          _asizes = new short[numColumns];
          asize = new short[numColumns];
          int k = 0;
          for (int i = 0; i < numColumns; i++) {
              _ctypes[i] = new AttrType(types[i].attrType);
              switch (types[i].attrType) {
                  case 0:
                      asize[i] = _asizes[i] = attrSizes[k];
                      asize[i] += 2;
                      k++;
                      break;
                  case 1:
                  case 2:
                      asize[i] = _asizes[i] = 4;
                      break;
                  case 3:
                      asize[i] = _asizes[i] = 1;
                      break;
                  case 4:
                      _asizes[i] = 0;
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
              hdr.setIntFld(2 + j, _ctypes[i].attrType);
              hdr.setIntFld(3 + j, _asizes[i]);
              hdr.setStrFld(4 + j, _fileName + i);
//              columnMap.put(colnames[i], i);
          }
          _hdrRid = hdrFile.insertRecord(hdr.returnTupleByteArray());
          BTMap = new HashMap<>();
          BMMap = new HashMap<>();
      }
  }

  public void deleteColumnarFile() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, FileAlreadyDeletedException, IOException {
    // Delete header file of the columnfile
    this._hdrFile.deleteFile();

    // Delete heapfile corresponding to each of the column in the column file
    for(int i = 0; i < this.numColumns; i++) {
      _columnHeaps[i].deleteFile();
    }

    // Set the number of columns to zero (because the column file is being deleted)
    this.numColumns = 0;

    this._fileDeleted = true;
  }

  public TID insertTuple(byte[] tuplePtr) throws Exception {
      int offset = getOffset();
      RID[] rids = new RID[numColumns];
      int position = 0;
      for (int i = 0; i < numColumns; i++) {

          byte[] data = new byte[asize[i]];
          System.arraycopy(tuplePtr, offset, data, 0, asize[i]);
          rids[i] = getColumn(i).insertRecord(data);
          offset += asize[i];
          if(i+1 == numColumns){
              position = getColumn(1).getRIDPos(rids[1]);
          }
      }
      TID tid = new TID(numColumns, position, rids);
      return tid;
  }

public int getOffset() {
    return 4 + (numColumns * 2);
}

  public Tuple getTuple(TID tid) throws Exception {
    RID[] rids = tid.recordIDs;
    int tupLen = this.tupLen;

    Tuple t = new Tuple(tupLen);
    byte[] res = t.getTupleByteArray();
    int offset = 0;
    for(int i = 0; i < numColumns; i++) {
      System.arraycopy(_columnHeaps[i].getRecord(rids[i]).getTupleByteArray(), 0, res, offset, _ctypes[i].attrSize);
      offset += _ctypes[i].attrSize;
    }
    t.tupleInit(res, 0, tupLen);

    return t;
  }

//  public ValueClass getValue(TID tid, int column) throws Exception {
//    Tuple t = getTuple(tid);
//    int offset = 0;
//    for(int i = 0; i < column; i++) {
//      offset += _asizes[i];
//    }
//    byte[] data = new byte[_asizes[column]];
//    System.arraycopy(t.getTupleByteArray(), offset, data, 0, _asizes[column]);
//    if(_ctypes[column].attrType == 0) {
//      IntegerVal value = new IntegerVal(data);
//      return value;
//    }
//
//    StringVal value = new StringVal(data);
//    return value;
//
//  }

  public boolean updateTuple(TID tid, Tuple newtuple) throws Exception {
    int offset = 0;
    byte[] values = newtuple.getTupleByteArray();
    for (int i = 0; i < numColumns; i++) {
        byte[] res = new byte[_asizes[i]];
        System.arraycopy(values, offset, res, 0, _asizes[i]);
        Tuple t = new Tuple(_asizes[i]);
        t.tupleInit(res, 0, res.length);
        _columnHeaps[i].updateRecord(tid.recordIDs[i], t);
        offset += _asizes[i];
    }
    return true;
}


  public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column) throws Exception {
    int offset = 0;
    byte[] values = newtuple.getTupleByteArray();
    Tuple t = new Tuple(_asizes[column]);
    byte[] res = t.getTupleByteArray();
    System.arraycopy(values, offset, res, 0, _asizes[column]);
    t.tupleInit(res, 0, res.length);
    _columnHeaps[column].updateRecord(tid.recordIDs[column], t);
    return true;
  }


  public boolean markTupleDeleted(TID tid) {
      return false;
  }

  boolean purgeAllDeletedTuples(){
      return true;
  }

  public int getTupleCnt() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, IOException {
    if(_columnHeaps.length > 0) return _columnHeaps[0].getRecCnt();
    return 0;
  }

  public boolean createBtreeIndex(int column) throws ConstructPageException, AddFileEntryException, GetFileEntryException, IOException, InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IteratorException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException, HFDiskMgrException, HFException {
    Heapfile colHeap = this.getColumn(column);
    System.out.println(colHeap);
    String index_name = colHeap.getHeapfileName() + ".btree";
    BTreeFile bTreeFile = new BTreeFile(index_name, _ctypes[column].attrType, _asizes[column], DeleteFashion.FULL_DELETE);
    Scan scan = new Scan(colHeap);

    Tuple t;
    RID rid = new RID();

    while(true) {
      t = scan.getNext(rid);
      if(t == null) break;
      System.out.println(t.getTupleByteArray());
      bTreeFile.insert(KeyFactory.getKeyClass(t.getTupleByteArray(), _ctypes[column], (short) asize[column]), rid);
    }
    scan.closescan();

    addIndexToColumnar(0, index_name);

    return true;
  }

//  public boolean createBitmapIndex(int column) throws Exception {
//    Heapfile colHeap = this.getColumn(column);
//    String indexName = colHeap.getHeapfileName() + ".btmap";
//    BitMapFile bitMapFile = new BitMapFile(indexName, this, column, value);
//    Tuple tuple;
//    int position = 0;

//    ColumnarColumnScan columnScan = new ColumnarColumnScan(getColumnarFileName(), columnNo,
//      projection,
//      targetedCols,
//      null, null);
//    while (true) {
//      tuple = columnScan.get_next();
//      if (tuple == null) {
//          break;
//      }
//      ValueClass valueClass = ValueFactory.getValueClass(tuple.getTupleByteArray(), atype[columnNo], asize[columnNo]);
//      if (valueClass.toString().equals(value.toString())) {
//          bitMapFile.insert(position);
//      } else {
//          bitMapFile.delete(position);
//      }
//      position++;
//    }
//  columnScan.close();
//  bitMapFile.close();

//  addIndexToColumnar(1, indexName);
//
//  return true;
//  }

    public Heapfile getColumn(int columnNo) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        if (_columnHeaps[columnNo] == null)
            _columnHeaps[columnNo] = new Heapfile(_fileName + columnNo);
        return _columnHeaps[columnNo];
    }

    private boolean addIndexToColumnar(int _idxType, String indexName) {

        try {
            AttrType[] _idxTypes = new AttrType[2];
            _idxTypes[0] = new AttrType(AttrType.attrInteger);
            _idxTypes[1] = new AttrType(AttrType.attrString);
            short[] _idxSizes = new short[1];
            _idxSizes[0] = 50; //index name can't be more than 50 chars
            Tuple t = new Tuple();
            t.setHdr((short) 2, _idxTypes, _idxSizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short) 2, _idxTypes, _idxSizes);
            t.setIntFld(1, _idxType);
            t.setStrFld(2, indexName);
            Heapfile f = new Heapfile(_fileName + ".idx");
            f.insertRecord(t.getTupleByteArray());

            if (_idxType == 0) {
                BTMap.put(indexName, null);
            } else if (_idxType == 1) {
                BMMap.put(indexName, null);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

   public AttrType[] getAttrtypes(){
    return _ctypes;
    }
    public short[] getAttrSize(){
      return _asizes;
    }

    public TupleScan openTupleScan() throws Exception {

        TupleScan result = new TupleScan(this);
        return result;
    }

    public TupleScan openTupleScan(short[] columns) throws Exception {
        TupleScan result = new TupleScan(this, columns);
        return result;
    }
   
}
