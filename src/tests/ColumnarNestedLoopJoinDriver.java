package tests;


import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;
import iterator.*;

public class ColumnarNestedLoopJoinDriver {

    private static String FILESCAN = "FILE";
    private static String COLUMNSCAN = "COLUMN";

    public static void main(String args[]) throws Exception {
        // columnDB cf1 cf2 "cf1.cf1.1,cf2.cf2.1" " " " " "FILE" " " "cf1.cf1.1,3" " " "cf2.1,cf2.3" "cf1.cf1.3=cf2.cf2.3" 20 100
        // columnDB cf1 cf1 “cf1.cf1.1,cf2.cf2.2” "cf1.cf1.3>4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.3" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.3” “cf1.cf1.3=cf2.cf2.3” 20 100
        // Working Query: java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf2.cf2.1,cf2.2.2,cf2.cf2.3" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3" "cf1.cf1.3 = cf2.cf2.3" 20 100
//        columnDB cf1 cf1 "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf2.cf2.1,cf2.2.2,cf2.cf2.3"
        // cf1.cf1.3 > 4
        // "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4"
        // FILE
        // cf1.cf1.3 > 4
        // "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3"
        // cf2.cf2.3 > 4
        // "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3"
        // cf1.cf1.3 = cf2.cf2.3
        // 20

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

        String dbpath = OperationUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        AttrType[] opAttr = new AttrType[projection.length];
        FldSpec[] projectionList = new FldSpec[projection.length];

        // Open the inner and outer relations
        Columnarfile outer = new Columnarfile(columnarFile1);
        Columnarfile inner = new Columnarfile(columnarFile2);

        // Find the projection lists on the join. Which columns (from the inner and the outer relation have to be included in the output print statements?)

        for (int i = 0; i < projection.length; i++) {
            String cfName = projection[i].split("\\.")[0];
            String attrName = projection[i].split("\\.")[2];
            if (cfName.equals(columnarFile1)) {
                for(int j = 0; j < outerTargetColumns.length; j++) System.out.println(outerTargetColumns[j]);
                projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), OperationUtils.getColumnPositionInTargets(attrName, outerTargetColumns) + 1);
                opAttr[i] = new AttrType(outer.getAttrtypeforcolumn(outer.getAttributePosition(attrName)).attrType);
            } else {
                projectionList[i] = new FldSpec(new RelSpec(RelSpec.innerRel), OperationUtils.getColumnPositionInTargets(attrName, innerTargetColumns) + 1);
                opAttr[i] = new AttrType(inner.getAttrtypeforcolumn(inner.getAttributePosition(attrName)).attrType);
            }
        }

        int[] scanCols = new int[outerScanColumns.length];
        for (int i = 0; i < outerScanColumns.length; i++) {
            if (!outerScanColumns[i].isEmpty()) {
                String attribute = OperationUtils.getAttributeName(outerScanColumns[i]);
                scanCols[i] = outer.getAttributePosition(attribute);
            }
        }

        short[] outerTargets = new short[outerTargetColumns.length];
        AttrType[] outerAttr = new AttrType[outerTargetColumns.length];
        FldSpec[] outerProjection = new FldSpec[outerTargetColumns.length];
        for (int i = 0; i < outerTargetColumns.length; i++) {
            String attribute = OperationUtils.getAttributeName(outerTargetColumns[i]);
            outerTargets[i] = (short) outer.getAttributePosition(attribute);
            outerProjection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            outerAttr[i] = new AttrType(outer.getAttrtypeforcolumn(outer.getAttributePosition(attribute)).attrType);
        }

        short[] innerTargets = new short[innerTargetColumns.length];
        AttrType[] innerAttr = new AttrType[outerTargetColumns.length];
        FldSpec[] innerProjection = new FldSpec[outerTargetColumns.length];
        for (int i = 0; i < innerTargetColumns.length; i++) {
            String attribute = OperationUtils.getAttributeName(innerTargetColumns[i]);
            innerTargets[i] = (short) inner.getAttributePosition(attribute);
            innerProjection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            innerAttr[i] = new AttrType(outer.getAttrtypeforcolumn(inner.getAttributePosition(attribute)).attrType);
        }

        CondExpr[] joinConst = OperationUtils.processEquiJoinConditionExpression(joinConstraints, innerTargetColumns, outerTargetColumns);
        CondExpr[] outerConst = OperationUtils.processRawConditionExpression(outerConstraints, outerTargetColumns);
        CondExpr[] innerConst = OperationUtils.processRawConditionExpression(innerConstraints, innerTargetColumns);

        CondExpr[][] scanConstraint = new CondExpr[outerScanTypes.length][1];

        for (int i = 0; i < outerScanTypes.length; i++) {
            scanConstraint[i] = OperationUtils.processRawConditionExpression(outerScanConstraints[i]);
        }

        Tuple proj_tuple = ColumnarScanUtils.getProjectionTuple(outer, inner, projectionList, innerTargets, outerTargets);

        inner.close();
        outer.close();
        Iterator it = null;
        Iterator cnlj = null;
        try {
            if (outerScanTypes[0].equals(FILESCAN)) {
                it = new ColumnarFileScan(columnarFile2, outerProjection, outerTargets, outerConst);
            } else if (outerScanTypes[0].equals(COLUMNSCAN)) {
                it = new ColumnarColumnScan(columnarFile2, scanCols[0], outerProjection, outerTargets, scanConstraint[0], outerConst);
            } else
                throw new Exception("Scan type <" + outerScanTypes[0] + "> not recognized.");

            cnlj = new ColumnarNestedLoopJoins(outerAttr, innerAttr, it, columnarFile1, joinConst, innerConst, innerTargets, innerProjection, projectionList, proj_tuple);
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

        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }
}
