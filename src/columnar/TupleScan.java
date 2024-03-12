package columnar;

import global.GlobalConst;
import global.TID;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;

public class TupleScan implements GlobalConst {
    Columnarfile _cf;
    TID[] _deletedTIDs;
    Scan[] sc;

    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException, IOException {
        init(cf);
    }

    //  Starts scan of columns using HeapFile APIs

    private void init(Columnarfile cf) throws InvalidTupleSizeException, IOException {
        sc = new Scan[cf.numColumns];
        _deletedTIDs = cf._deletedTuples;

        // TODO: Add a hashmap for deleted TIDS
        for(int i = 0; i < cf.numColumns; i++) {
            sc[i] = cf._columnHeaps[i].openScan();
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
