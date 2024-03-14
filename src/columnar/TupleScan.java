package columnar;

import global.AttrType;
import global.GlobalConst;
import global.TID;
import heap.*;

import java.io.IOException;

public class TupleScan implements GlobalConst {
    Columnarfile _cf;
    int numColumns;
    AttrType[] atype;
    short[] asize;
    short[] strSize;
    int toffset;
    int tuplesize;
    TID[] _deletedTIDs;
    Scan[] sc;

    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException, IOException, HFDiskMgrException, HFException, HFBufMgrException {
        init(cf);
    }

    public TupleScan(Columnarfile f,short[] columns) throws Exception {
        _cf = f;
        numColumns = (short)columns.length;
        atype = new AttrType[numColumns];
        asize = new short[numColumns];
        sc=new Scan[numColumns];
        short strCnt = 0;
        for(int i=0;i<numColumns;i++){

            short c = (short) (columns[i] - 1);
            atype[i] = f._ctypes[c];
            asize[i] = (short) f.asize[c];
            sc[i] = f.getColumn(c).openScan();

            if(atype[i].attrType == AttrType.attrString)
                strCnt++;
        }

        strSize = new short[strCnt];
        toffset = 4 + (numColumns * 2);
        tuplesize = toffset;
        int cnt = 0;
        for(int i = 0; i < numColumns; i++){
            short c = columns[i];
            if(atype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = (short) f._asizes[c];
            }
            tuplesize += asize[i];
        }
    }

    //  Starts scan of columns using HeapFile APIs

    private void init(Columnarfile cf) throws InvalidTupleSizeException, IOException, HFDiskMgrException, HFException, HFBufMgrException {
        sc = new Scan[cf.numColumns];
        _deletedTIDs = cf._deletedTuples;

        // TODO: Add a hashmap for deleted TIDS
        for(int i = 0; i < cf.numColumns; i++) {
            sc[i] = cf.getColumn(i).openScan();
        }
    }

    // Retrieve the next tuple in a sequential scan
    public Tuple getNext(TID tid) {
        return new Tuple();
    }
    //  Position all scan cursors to the records with the given rids
    boolean position(TID tid) throws InvalidTupleSizeException, IOException {
        for(int i = 0; i < tid.numRIDs; i++) {
            if(! sc[i].position(tid.recordIDs[i])) return false;
        }
        return true;
    }

    public void closetuplescan(){
		for(int i=0;i<sc.length;i++){
			sc[i].closescan();
		}
        // file.close();
	}

}
