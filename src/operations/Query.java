package operations;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.Tuple;
import iterator.*;

public class Query {
    private static String FILESCAN = "FILE";
    private static String COLUMNSCAN = "COLUMN";
    private static String BITMAPSCAN = "BITMAP";
    private static String BTREESCAN = "BTREE";

    public static void main(String[] args) throws Exception {
        // Query Skeleton: COLUMNDB COLUMNFILE PROJECTION OTHERCONST SCANCOLS [SCANTYPE] [SCANCONST] TARGETCOLUMNS NUMBUF SORTMEM
        // Example Query: testColumnDB columnarTable A,B,C "C = 5" A,B [BTREE,BITMAP] "(A = 5 v A = 6),(B > 7)" A,B,C 100 0
//        testColumnDB columnarTable A,B,C "C = 5" A,B [BTREE,BITMAP] "(A = 5 v A = 6),(B > 7)" A,B,C 100 0
        // In case no constraints need to be applied, pass "" as input.
        String columnDB = args[0];
        String columnarFile = args[1];
        String otherConstraints = args[2];
        String[] scanColumns = args[3].split(",");
        String[] scanTypes = args[4].split(",");
        String[] scanConstraints = args[5].split(",");
        String[] targetColumns = args[6].split(",");
        Integer bufferSize = Integer.parseInt(args[7]);
        Integer sortmem = Integer.parseInt(args[8]);

        String dbpath = OperationUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        runOperation(columnarFile, otherConstraints, scanColumns, scanTypes, scanConstraints, targetColumns, sortmem);

//        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Read count: " + PCounter.rcounter + "\nWrite count: " + PCounter.wcounter);
    }

    private static void runOperation(String columnarFile, String otherConstraints, String[] scanColumns, String[] scanTypes, String[] scanConstraints, String[] targetColumns, int sortmem) throws Exception {
        Columnarfile cf = new Columnarfile(columnarFile);

        int[] scanCols = new int[scanColumns.length];
        for (int i = 0; i < scanColumns.length; i++) {
            if (!scanColumns[i].equals("")) {
                scanCols[i] = Integer.parseInt(scanColumns[i].split("\\.")[1]);
            }
        }

        short[] targets = new short[targetColumns.length];

        for (int i = 0; i < targetColumns.length; i++) {
            System.out.println(targetColumns[i].split("\\.")[1]);
            targets[i] = Short.parseShort(targetColumns[i].split("\\.")[1]);
        }

        CondExpr[] otherConstraint = OperationUtils.processRawConditionExpression(otherConstraints, targetColumns);

        CondExpr[][] scanConstraint = new CondExpr[scanTypes.length][1];

        for (int i = 0; i < scanTypes.length; i++) {
            scanConstraint[i] = OperationUtils.processRawConditionExpression(scanConstraints[i], targetColumns);
        }

        Iterator it = null;

        FldSpec[] projectionList = new FldSpec[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++) {
            String attribute = OperationUtils.getAttributeName(targetColumns[i]);
            projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), OperationUtils.getColumnPositionInTargets(attribute, targetColumns) + 1);
        }

        try {
            it = new ColumnarFileScan(columnarFile, projectionList, targets, otherConstraint);
            int c = 0;
            while(true) {
                Tuple res = it.get_next();
                if (res == null) {
                    break;
                }
                c++;
                res.print(cf.getAttrtypes());
            }

            System.out.println();
            System.out.println(c + " tuples selected");
            System.out.println();

        } catch(Exception e) {
            e.printStackTrace();
        }

    }
}