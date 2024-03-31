package tests;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.SystemDefs;

public class Index {

    public static void main(String[] args) throws Exception {
        // Query Skeleton: COLUMNDB COLUMNARFILE COLUMNNAME INDEXTYPE
        // Example Query: testColumnDB columnarTable columnName BITMAP|BTREE
        String columnDB = args[0];
        String columnarFile = args[1];
        String num = args[2];
        String indexType = args[3];

        String dbpath = OperationUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, 1000000, "Clock");

        runInterface(columnarFile, num, indexType);

//        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String num, String indexType) throws Exception {
       Columnarfile cf = new Columnarfile(columnarFile);

       if (indexType.equals("BITMAP")) {
           cf.createAllBitMapIndexForColumn(Integer.parseInt(num));
           System.out.println("no bitmap");
       } else {
           cf.createBtreeIndex(Integer.parseInt(num));
       }

        System.out.println(indexType + " created successfully on "+columnarFile+"."+num);
    }
}