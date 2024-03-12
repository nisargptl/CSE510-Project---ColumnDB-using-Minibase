package columnar;

import btree.*;
import com.sun.jdi.IntegerValue;
import diskmgr.Page;
import global.*;
import heap.*;
import org.w3c.dom.Attr;

import java.io.IOException;

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
  int[] _asizes;
  // Length of a tuple in the column store
  int tupLen;

  public Columnarfile(String name, AttrType[] types, int numColumns) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, Exception {
    this._fileName = name;
    this.numColumns = numColumns;
    this._ctypes = types;
    this._asizes = new int[numColumns];
    this.tupLen = 0;

    this._columnHeaps = new Heapfile[numColumns];
    // Instantiating one heapfile for each column
    for(int i = 0; i < numColumns; i++) {
      _columnHeaps[i] = new Heapfile(name + i);
      _asizes[i] = types[i].attrSize;
      tupLen += types[i].attrSize;
    }
    // Creating the header file for the columnarfile
    this._hdrFile = new Heapfile(name + ".hdr");

    AttrType[] headerTypes = new AttrType[2 + (numColumns * 3)];
      headerTypes[0] = new AttrType(AttrType.attrInteger);

      for (int i = 1; i < headerTypes.length - 1; i = i + 3) {
          headerTypes[i] = new AttrType(AttrType.attrInteger);
          headerTypes[i + 1] = new AttrType(AttrType.attrInteger);
          headerTypes[i + 2] = new AttrType(AttrType.attrString);
      }

      headerTypes[headerTypes.length - 1] = new AttrType(AttrType.attrInteger);

      short[] headerSizes = new short[numColumns];
      for (int i = 0; i < numColumns; i++) {
          // Column size cant be more than 25 chars
          headerSizes[i] = 25;
      }
      Tuple hdr = new Tuple();
      hdr.setHdr((short) headerTypes.length, headerTypes, headerSizes);
      int size = hdr.size();

      hdr = new Tuple(size);
      hdr.setHdr((short) headerTypes.length, headerTypes, headerSizes);
      hdr.setIntFld(1, numColumns);
      int j = 0;
      for (int i = 0; i < numColumns; i++, j = j + 3) {
          hdr.setIntFld(2 + j, _ctypes[i].attrType);
          hdr.setIntFld(3 + j, _asizes[i]);
          hdr.setStrFld(4 + j, name + i);
//          columnMap.put(colnames[i], i);
      }
      _hdrRid = _hdrFile.insertRecord(hdr.returnTupleByteArray());
    //  Add BTIndex and BMIndex
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
    int offset = 0;
    int position = 0;
    RID[] rids = new RID[numColumns];

    for(int i = 0; i < this.numColumns; i++) {
      int length = _ctypes[i].attrSize;
      byte[] data = new byte[length];
      System.arraycopy(tuplePtr, offset, data, 0, length);
      rids[i] = _columnHeaps[i].insertRecord(data);
      offset += length;
      System.out.println(rids[i]);
      position = _columnHeaps[i].getRIDPos(rids[i]);
    }

    return new TID(numColumns, position, rids);
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

  public boolean createBtreeIndex(int column) throws ConstructPageException, AddFileEntryException, GetFileEntryException, IOException, InvalidTupleSizeException, InvalidSlotNumberException, IteratorException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException, HFException, HFDiskMgrException, HFBufMgrException {
    _columnHeaps[column] = new Heapfile(_fileName + column);
    Heapfile colHeap = _columnHeaps[column];
    String index_name = colHeap.getHeapfileName() + ".btree";
    BTreeFile bTreeFile = new BTreeFile(index_name, _ctypes[column].attrType, _asizes[column], DeleteFashion.FULL_DELETE);
    Scan scan = new Scan(colHeap);

    Tuple t;

    // Setup to scan the column heapfile
    PageId currentDirPageId = new PageId(colHeap.getFirstDirPageId().pid);
    PageId nextDirPageId = new PageId(0);
    HFPage currentDirPage = new HFPage();
    Page pageInBuffer = new Page();

    while(currentDirPageId.pid != INVALID_PAGE) {
      colHeap.pinPage(currentDirPageId, currentDirPage, false);
      RID rid = new RID();
      for(rid = currentDirPage.firstRecord(); rid != null; rid = currentDirPage.nextRecord(rid)) {
        t = currentDirPage.getRecord(rid);
        KeyClass k = KeyFactory.getKeyClass(t.getTupleByteArray(), _ctypes[column], (short) _asizes[column]);
          try {
              bTreeFile.insert(k, rid);
          } catch (Exception e) {
            return false;
          }
      }
    }
    return true;
  }
}
