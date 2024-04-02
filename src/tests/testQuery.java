package tests;

import btree.*;
import columnar.Columnarfile;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.IOException;

public class testQuery {


    public testQuery() throws HFDiskMgrException, HFException, HFBufMgrException, IOException {
    }

    public static void main(String[] args) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, ConstructPageException, GetFileEntryException, PinPageException, IteratorException, KeyNotMatchException, UnpinPageException, ScanIteratorException {
        String dbpath = OperationUtils.dbPath("name");
        SystemDefs sysdef = new SystemDefs(dbpath, 0, 50, "Clock");
        Columnarfile cf = new Columnarfile("name");

        String indexName = cf.getBTName(2);
        System.out.println(indexName);

        BTreeFile f = new BTreeFile(indexName);
        BTFileScan scan = f.new_scan(new IntegerKey(2), new IntegerKey(10));
        while(true) {
            KeyDataEntry entry = scan.get_next();
            if (entry != null)
                System.out.println(entry.key + " " + entry.data);
        }
    }
}
