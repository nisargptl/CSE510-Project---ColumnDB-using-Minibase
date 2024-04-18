package tests;


import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.Tuple;
import index.ColumnarIndexScan;
import iterator.*;

public class Joins {

    private static String FILESCAN = "FILE";
    private static String COLUMNSCAN = "COLUMN";
    private static String BITMAPSCAN = "BITMAP";
    private static String BTREESCAN = "BTREE";

    public static void main(String args[]) throws Exception {
        // Query Skeleton: COLUMNDB CF1 CF2 PROJECTION OUTERCONST OUTERSCANCOLS [OUTERSCANTYPE] [OUTERSCANCONST] OUTERTARGETCOLUMNS INNERCONST INNERTARGETCOLUMNS JOINCONDITION NUMBUF
        // columnDB cf1 cf2 "cf1.cf1.1,cf2.cf2.1" " " " " "FILE" " " "cf1.cf1.1,3" " " "cf2.1,cf2.3" "cf1.cf1.3=cf2.cf2.3" 20 100
        // columnDB cf1 cf1 “cf1.cf1.1,cf2.cf2.2” "cf1.cf1.3>4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.3" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.3” “cf1.cf1.3=cf2.cf2.3” 20 100

        String columnDB = args[0];
        String columnarFile1 = args[1];
        String columnarFile2 = args[2];
        String[] projection = args[3].split(",");
        String outerConstraints = args[4];
        String[] outerScanColumns = args[5].split(",");
        String[] outerScanTypes = args[6].split(",");
        String[] outerScanConstraints = args[7].split(",");
        String[] outerTargetColumns = args[8].split(",");
        String innerConstraints = args[9];
        String[] innerTargetColumns = args[10].split(",");
        String joinConstraints = args[11];
        Integer bufferSize = Integer.parseInt(args[12]);
        Integer sortmem = Integer.parseInt(args[13]);

        String dbpath = OperationUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        runInterface(columnarFile1, columnarFile2, projection, outerConstraints, outerScanColumns, outerScanTypes, outerScanConstraints, outerTargetColumns, innerConstraints, innerTargetColumns, joinConstraints, sortmem);

//        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String ocf, String icf, String[] projection, String outerConstraints, String[] outerScanColumns, String[] outerScanTypes, String[] outerScanConstraints, String[] outerTargetColumns, String innerConstraints, String[] innerTargetColumns, String joinConstraints, int sortmem) throws Exception {

        Columnarfile outer = new Columnarfile(ocf);
        Columnarfile inner = new Columnarfile(icf);

        AttrType[] opAttr = new AttrType[projection.length];
        FldSpec[] projectionList = new FldSpec[projection.length];
        for (int i = 0; i < projection.length; i++) {
            String attribute = projection[i].split("\\.")[2];
            String relationName = projection[i].split("\\.")[0];
            if (relationName.equals(ocf)) {
                System.out.println("ATTR: " +  attribute);
                for(int j = 0; j < outerTargetColumns.length; j++) System.out.println(outerTargetColumns[j]);
                projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), OperationUtils.getColumnPositionInTargets(attribute, outerTargetColumns) + 1);
                opAttr[i] = new AttrType(outer.getAttrtypeforcolumn(outer.getAttributePosition(attribute)).attrType);
            } else {
                projectionList[i] = new FldSpec(new RelSpec(RelSpec.innerRel), OperationUtils.getColumnPositionInTargets(attribute, innerTargetColumns) + 1);
                opAttr[i] = new AttrType(inner.getAttrtypeforcolumn(inner.getAttributePosition(attribute)).attrType);
            }
        }

        int[] scanCols = new int[outerScanColumns.length];
        System.out.println("OUTER: " + outerScanColumns[0] + " " + outerScanColumns.length);
        for (int i = 0; i < outerScanColumns.length; i++) {
            if (!outerScanColumns[i].equals("")) {
                String attribute = OperationUtils.getAttributeName(outerScanColumns[i]);
                System.out.println("ATTRIBUTE: " + attribute);
                System.out.println("SCAN COL: " + outerScanColumns[i]);
                scanCols[i] = outer.getAttributePosition(attribute);
            }
        }

        short[] outertargets = new short[outerTargetColumns.length];
        AttrType[] outerAttr = new AttrType[outerTargetColumns.length];
        FldSpec[] outerProjection = new FldSpec[outerTargetColumns.length];
        for (int i = 0; i < outerTargetColumns.length; i++) {
            String attribute = OperationUtils.getAttributeName(outerTargetColumns[i]);
            outertargets[i] = (short) outer.getAttributePosition(attribute);
            outerProjection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            outerAttr[i] = new AttrType(outer.getAttrtypeforcolumn(outer.getAttributePosition(attribute)).attrType);
        }

        short[] innertargets = new short[innerTargetColumns.length];
        AttrType[] innerAttr = new AttrType[outerTargetColumns.length];
        FldSpec[] innerProjection = new FldSpec[outerTargetColumns.length];
        for (int i = 0; i < innerTargetColumns.length; i++) {
            String attribute = OperationUtils.getAttributeName(innerTargetColumns[i]);
            innertargets[i] = (short) inner.getAttributePosition(attribute);
            innerProjection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            innerAttr[i] = new AttrType(outer.getAttrtypeforcolumn(inner.getAttributePosition(attribute)).attrType);
        }

        CondExpr[] outerConstraint = OperationUtils.processRawConditionExpression(outerConstraints, outerTargetColumns);
        CondExpr[] innerConstraint = OperationUtils.processRawConditionExpression(innerConstraints, innerTargetColumns);
        CondExpr[] joinConstraint = OperationUtils.processEquiJoinConditionExpression(joinConstraints, innerTargetColumns, outerTargetColumns);

        CondExpr[][] scanConstraint = new CondExpr[outerScanTypes.length][1];

        for (int i = 0; i < outerScanTypes.length; i++) {
            scanConstraint[i] = OperationUtils.processRawConditionExpression(outerScanConstraints[i]);
        }

        Tuple proj_tuple = ColumnarScanUtils.getProjectionTuple(outer, inner, projectionList, innertargets, outertargets);

        inner.close();
        outer.close();
        Iterator it = null;
        Iterator cnlj = null;
        try {
            if (outerScanTypes[0].equals(FILESCAN)) {
                it = new ColumnarFileScan(ocf, outerProjection, outertargets, outerConstraint);
            } else if (outerScanTypes[0].equals(COLUMNSCAN)) {
                it = new ColumnarColumnScan(ocf, scanCols[0], outerProjection, outertargets, scanConstraint[0], outerConstraint);
//            } else if (outerScanTypes[0].equals(BITMAPSCAN) || outerScanTypes[0].equals(BTREESCAN)) {
//                IndexType[] indexType = new IndexType[outerScanTypes.length];
//                for (int i = 0; i < outerScanTypes.length; i++) {
//                    if (outerScanTypes[i].equals(BITMAPSCAN))
//                        indexType[i] = new IndexType(IndexType.BitMapIndex);
//                    else if (outerScanTypes[i].equals(BTREESCAN))
//                        indexType[i] = new IndexType(IndexType.B_Index);
//                    else
//                        throw new Exception("Scan type <" + outerScanTypes[i] + "> not recognized.");
//                }
//                it = new ColumnarIndexScan(ocf, scanCols, indexType, scanConstraint, outerConstraint, false, outertargets, outerProjection, sortmem);
            } else
                throw new Exception("Scan type <" + outerScanTypes[0] + "> not recognized.");

            cnlj = new ColumnarNestedLoopJoins(outerAttr, innerAttr, it, icf, joinConstraint, innerConstraint, innertargets, innerProjection, projectionList, proj_tuple);
            int cnt = 0;
            while (true) {
                Tuple result = cnlj.get_next();
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
            cnlj.close();
            it.close();
        }
    }
}
