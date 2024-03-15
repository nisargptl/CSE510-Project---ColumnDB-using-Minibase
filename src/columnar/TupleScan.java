package columnar;

import global.AttrType;
import global.RID;
import global.TID;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

import java.awt.image.BaseMultiResolutionImage;
import java.io.IOException;

public class TupleScan {
    Scan[] scans;
    private Columnarfile _columnarFile;

    private AttrType[] _ctypes;
    private short[] _csize;
    private short[] _stringSizes;
    private short numColumns;
    private int tupOffset;
    private int tupSize;

    // Functionality - Scans only the columns mentioned in the columns array
    // Parameters -
    // 1. Columnarfile f - This is the columnar file object on which we initiate scans.
    // 2. short[] columns - contains the list of column numbers on which the scans have to be initiated
    // Returns - nothing
    public TupleScan(Columnarfile _cFile,short[] columns) throws Exception {
        _columnarFile = _cFile;
        numColumns = (short)columns.length;
        _ctypes = new AttrType[numColumns];
        _csize = new short[numColumns];
        scans = new Scan[numColumns];
        short strCnt = 0;

        for(int i=0;i<numColumns;i++){
            scans[i] = _columnarFile.getColumn(columns[i]).openScan();

            // Store the type and size information into the tuplescan object
            _ctypes[i] = _columnarFile.atype[columns[i]];
            _csize[i] = _columnarFile.asize[columns[i]];

            if(_ctypes[i].attrType == AttrType.attrString)
                strCnt++;
        }

        _stringSizes = new short[strCnt];
        tupOffset = 4 + (numColumns * 2);
        tupSize = tupOffset;
        int cnt = 0;
        for(int i = 0; i < numColumns; i++){
            short c = columns[i];
            if(_ctypes[i].attrType == AttrType.attrString) {
                _stringSizes[cnt++] = _columnarFile.attrsizes[c];
            }
            tupSize += _csize[i];
        }
    }

    // Functionality - Start scan on all columns
    // Parameters -
    // 1. Columnarfile _file - the columnar file object on which we have to open the tuplescan
    // Returns - Nothing
    public TupleScan(Columnarfile _file) throws Exception {
        scans = new Scan[numColumns];

        _columnarFile = _file;
        _ctypes = _file.atype;
        _csize = _file.asize;
        numColumns = _file.numColumns;

        _stringSizes = _file.getStrSize();

        tupOffset = _file.getOffset();
        tupSize = _file.getTupleSize();

        for(int i=0;i<numColumns;i++){
            scans[i] = _file.getColumn(i).openScan();
        }
    }

    // Functionality - Gets the next tuple in the columnarfile
    // Parameters -
    // 1. TID tid - A TID object which is the predecessor of the tuple which we plan to get.
    // Returns - A Tuple object which contains the Tuple next to the TID provided in the param.
    public Tuple getNext(TID tid) throws Exception {
        Tuple res = new Tuple(tupSize);
        res.setHdr(numColumns, _ctypes, _stringSizes);
        byte[] data = res.getTupleByteArray();

        RID[] rids = new RID[scans.length];

        RID rid = new RID();

        int position = 0;

        int offset = tupOffset;

        for (int i = 0; i < numColumns; i++) {
            Tuple t = scans[i].getNext(rid);
            if (t == null)
                return null;

            rids[i] = new RID();
            rids[i].copyRid(rid);
            rid = new RID();
            int size = _csize[i];
            System.arraycopy(t.getTupleByteArray(), 0, data, offset, size);
            offset += _csize[i];
            if(i+1 == numColumns)
                position = scans[i].positionOfRecord(rids[i]);
        }

        tid.numRIDs = scans.length;
        tid.recordIDs = rids;
        tid.setPosition(position);
        res.tupleInit(data, 0, data.length);

        return res;
    }

    // Functionality - checks if the position of the tuple scan object is at the TID provided as the param
    // Parameters -
    // 1. TID tid - TID object with which the position of the tuplescan is being compared
    // Returns - Boolean
    public boolean position(TID tid){
        for(int i=0; i<tid.numRIDs; i++){
            try {
                if(!scans[i].position(tid.recordIDs[i])) return false;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    // Functionality - Close all the scans objects opened.
    // Parameters - None
    // Returns - Nothing
    public void closetuplescan(){
        for(int i = 0; i < scans.length; i++){
            scans[i].closescan();
        }
        _columnarFile.close();
    }
}