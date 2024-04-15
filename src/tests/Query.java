package tests;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.Tuple;
import index.ColumnarIndexScan;
import iterator.*;

public class Query {

    private static final String FILESCAN = "FILE";
    private static final String COLUMNSCAN = "COLUMN";
    private static final String BITMAPSCAN = "BITMAP";
    private static final String BTREESCAN = "BTREE";

    public static void main(String[] args) throws Exception {
        // Query Skeleton: COLUMNDB COLUMNFILE PROJECTION OTHERCONST SCANCOLS [SCANTYPE]
        // [SCANCONST] TARGETCOLUMNS NUMBUF SORTMEM
        // Example Query: testColumnDB columnarTable A,B,C "C = 5" A,B [BTREE,BITMAP]
        // "(A = 5 v A = 6),(B > 7)" A,B,C 100 0
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

        runInterface(columnarFile, projection, otherConstraints, scanColumns, scanTypes, scanConstraints, targetColumns,
                sortmem);

        // SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String[] projection, String otherConstraints,
            String[] scanColumns, String[] scanTypes, String[] scanConstraints, String[] targetColumns, int sortmem)
            throws Exception {

        Columnarfile cf = new Columnarfile(columnarFile);
        System.out.println("here");
        AttrType[] opAttr = cf.getAttributes();
        FldSpec[] projectionList = new FldSpec[projection.length];
        short[] str_sizes = cf.getStrSize();
        String[] indName = new String[scanColumns.length];

        for (int i = 0; i < projection.length; i++) {
            String attribute = OperationUtils.getAttributeName(projection[i]);
            projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer),
                    OperationUtils.getColumnPositionInTargets(attribute, targetColumns) + 1);
            opAttr[i] = new AttrType(cf.getAttrtypeforcolumn(cf.getAttributePosition(attribute)).attrType);
        }

        int[] scanCols = new int[scanColumns.length];

        for (int i = 0; i < scanColumns.length; i++) {
            System.out.println(scanColumns[i]);
            if (!scanColumns[i].equals("")) {
                String attribute = OperationUtils.getAttributeName(scanColumns[i]);
                scanCols[i] = cf.getAttributePosition(attribute); // todo changed from
                                                                  // cf.getAttributePosition(attribute) + 1;
                indName[i] = cf.getBTName(scanCols[i]);
            }
        }

        for (int i = 0; i < scanCols.length; i++) {
            System.out.println("ScanCols[" + i + "]: " + scanCols[i]);
        }

        short[] targets = new short[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++) {
            String attribute = OperationUtils.getAttributeName(targetColumns[i]);
            targets[i] = (short) cf.getAttributePosition(attribute);
        }

        CondExpr[] otherConstraint = OperationUtils.processRawConditionExpression(otherConstraints, targetColumns);

        CondExpr[][] scanConstraint = new CondExpr[scanTypes.length][1];

        for (int i = 0; i < scanConstraints.length; i++) { // todo: changed this
            scanConstraint[i] = OperationUtils.processRawConditionExpression(scanConstraints[i]);
        }
        cf.close();
        Iterator it = null;
        try {
            if (scanTypes[0].equals(FILESCAN)) {
                it = new ColumnarFileScan(columnarFile, projectionList, targets, otherConstraint);
            } else if (scanTypes[0].equals(COLUMNSCAN)) {
                it = new ColumnarColumnScan(columnarFile, scanCols[0], projectionList, targets, scanConstraint[0],
                        otherConstraint);
            } else if (scanTypes[0].equals(BTREESCAN) || scanTypes[0].equals(BITMAPSCAN)) {
                IndexType[] indexType = new IndexType[scanTypes.length];
                String[] indexName = new String[scanTypes.length];
                for (int i = 0; i < scanTypes.length; i++) {
                    if (scanTypes[i].equals(BITMAPSCAN)) {
                        indexType[i] = new IndexType(IndexType.BitMapIndex);
                        // indexName[i] = cf.getBMName();
                    } else if (scanTypes[i].equals(BTREESCAN)) {
                        indexType[i] = new IndexType(IndexType.B_Index);
                        indexName[i] = cf.getBTName(i); // todo: why is this based on i?
                    } else {
                        indexType[i] = new IndexType(IndexType.None);
                    }
                }

                System.out.println("Index Type len: " + indexType.length);
                System.out.println("Index Name len: " + indexName.length);
                System.out.println("Fldnum len: " + scanCols.length);
                System.out.println("str sizes len: " + str_sizes.length);
                System.out.println("Projection len: " + projection.length);

                it = new ColumnarIndexScan(columnarFile, scanCols, indexType, indName, opAttr, str_sizes,
                        scanColumns.length, projection.length, projectionList, otherConstraint, true);
            } else
                throw new Exception("Scan type <" + scanTypes[0] + "> not recognized.");
            System.out.println("here");
            int cnt = 0;
            while (true) {
                System.out.println(cnt);
                Tuple result = it.get_next();
                // System.out.println("Count");
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
