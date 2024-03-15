package operations;


import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;
import iterator.*;

public class Query {

    private static String FILESCAN = "FILE";
    private static String COLUMNSCAN = "COLUMN";
    private static String BITMAPSCAN = "BITMAP";
    private static String BTREESCAN = "BTREE";

    public static void main(String args[]) throws Exception {
        // Query Skeleton: COLUMNDB COLUMNFILE PROJECTION OTHERCONST SCANCOLS [SCANTYPE] [SCANCONST] TARGETCOLUMNS NUMBUF SORTMEM
        // Example Query: testColumnDB columnarTable A,B,C "C = 5" A,B [BTREE,BITMAP] "(A = 5 v A = 6),(B > 7)" A,B,C 100 0
        // In case no constraints need to be applied, pass "" as input.
        String columnDB = args[0];
        String columnarFile = args[1];
        String[] projection = args[2].split(",");
        String otherConstraints = args[3];
        String[] scanColumns = args[4].split(",");
        String[] scanTypes = args[5].split(",");
        String[] scanConstraints = args[6].split(",");
        String[] targetColumns = args[7].split(",");
        Integer bufferSize = Integer.parseInt(args[8]);
        Integer sortmem = Integer.parseInt(args[9]);

        String dbpath = OperationUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        runInterface(columnarFile, projection, otherConstraints, scanColumns, scanTypes, scanConstraints, targetColumns, sortmem);

//        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String[] projection, String otherConstraints, String[] scanColumns, String[] scanTypes, String[] scanConstraints, String[] targetColumns, int sortmem) throws Exception {

        Columnarfile cf = new Columnarfile(columnarFile);
        System.out.println("here");
        AttrType[] opAttr = new AttrType[projection.length];
        FldSpec[] projectionList = new FldSpec[projection.length];
        for (int i = 0; i < projection.length; i++) {
            String attribute = OperationUtils.getAttributeName(projection[i]);
            projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), OperationUtils.getColumnPositionInTargets(attribute, targetColumns) + 1);
            opAttr[i] = new AttrType(cf.getAttrtypeforcolumn(cf.getAttributePosition(attribute)).attrType);
        }

        int[] scanCols = new int[scanColumns.length];
        for (int i = 0; i < scanColumns.length; i++) {
            if (!scanColumns[i].equals("")) {
                String attribute = OperationUtils.getAttributeName(scanColumns[i]);
                scanCols[i] = cf.getAttributePosition(attribute);
            }
        }

        short[] targets = new short[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++) {
            String attribute = OperationUtils.getAttributeName(targetColumns[i]);
            targets[i] = (short) cf.getAttributePosition(attribute);
        }


        CondExpr[] otherConstraint = OperationUtils.processRawConditionExpression(otherConstraints, targetColumns);

        CondExpr[][] scanConstraint = new CondExpr[scanTypes.length][1];

        for (int i = 0; i < scanTypes.length; i++) {
            scanConstraint[i] = OperationUtils.processRawConditionExpression(scanConstraints[i]);
        }
        cf.close();
        Iterator it = null;
        try {
            if (scanTypes[0].equals(FILESCAN)) {
                it = new ColumnarFileScan(columnarFile, projectionList, targets, otherConstraint);
//            } else if (scanTypes[0].equals(COLUMNSCAN)) {
//                it = new ColumnarColumnScan(columnarFile, scanCols[0], projectionList, targets, scanConstraint[0], otherConstraint);
//            } else if (scanTypes[0].equals(BITMAPSCAN) || scanTypes[0].equals(BTREESCAN)) {
//                IndexType[] indexType = new IndexType[scanTypes.length];
//                for (int i = 0; i < scanTypes.length; i++) {
//                    if (scanTypes[i].equals(BITMAPSCAN))
//                        indexType[i] = new IndexType(IndexType.BitMapIndex);
//                    else if (scanTypes[i].equals(BTREESCAN))
//                        indexType[i] = new IndexType(IndexType.B_Index);
//                    else
//                        throw new Exception("Scan type <" + scanTypes[i] + "> not recognized.");
//                }
//
//                it = new ColumnarIndexScan(columnarFile, scanCols, indexType, , scanConstraint, otherConstraint, false, targets, projectionList, sortmem);
            } else
                throw new Exception("Scan type <" + scanTypes[0] + "> not recognized.");
            System.out.println("here");
            int cnt = 0;
            while (true) {
                Tuple result = it.get_next();
                if (result == null) {
                    break;
                }
                cnt++;
                result.print(opAttr);
            }

            System.out.println();
            System.out.println(cnt + " tuples selected");
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            it.close();
        }
    }
}

