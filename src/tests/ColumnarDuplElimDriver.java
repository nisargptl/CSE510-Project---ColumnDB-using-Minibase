package tests;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import global.TupleOrder;
import heap.Tuple;
import index.ColumnarIndexScan;
import iterator.*;

import java.util.ArrayList;

public class ColumnarDuplElimDriver {
    private static final String FILESCAN = "FILE";
    private static final String COLUMNSCAN = "COLUMN";
    private static final String BITMAPSCAN = "BITMAP";
    private static final String BTREESCAN = "BTREE";

    public static void main(String[] args) throws Exception {
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

        String columndbpath = OperationUtils.dbPath(columnDB);
        new SystemDefs(columndbpath, 0, bufferSize, "Clock");

        runInterface(columnarFile, projection, otherConstraints, scanColumns, scanTypes, scanConstraints, targetColumns, sortmem);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String[] projection, String otherConstraints, String[] scanColumns, String[] scanTypes, String[] scanConstraints, String[] targetColumns, int sortmem) throws Exception {
        Columnarfile cf = new Columnarfile(columnarFile);
        AttrType[] opAttr = cf.getAttributes();
        FldSpec[] projectionList = new FldSpec[projection.length];
        short[] str_sizes = cf.getStrSize();
        String[] indName = new String[scanColumns.length];

        for (int i = 0; i < projection.length; i++) {
            String attribute = OperationUtils.getAttributeName(projection[i]);
            projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), OperationUtils.getColumnPositionInTargets(attribute, targetColumns) + 1);
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

        for (int i = 0; i < scanTypes.length; i++) {
            scanConstraint[i] = OperationUtils.processRawConditionExpression(scanConstraints[i]);
        }
        cf.close();
        ArrayList<Iterator> it = new ArrayList<Iterator>();
        Iterator it1 = null;
        try {
            if (scanTypes[0].equals(FILESCAN)) {
                for (int i = 0; i < 2; i++) {
                    it.add(new ColumnarFileScan(columnarFile, projectionList, targets, otherConstraint));
                }
            } else if (scanTypes[0].equals(COLUMNSCAN)) {
                for (int i = 0; i < 2; i++) {
                    it.add(new ColumnarColumnScan(columnarFile, scanCols[0], projectionList, targets, scanConstraint[0], otherConstraint));
                }
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
                for (int i = 0; i < 2; i++) {
                    it.add(new ColumnarIndexScan(columnarFile, scanCols, indexType, indName, opAttr, str_sizes, scanColumns.length, projection.length, projectionList, otherConstraint, true));
                }

            } else {
                throw new Exception("Scan type <" + scanTypes[0] + "> not recognized.");
            }

            int cnt = 0;
            it1 = new ColumnarDuplElim(cf.getAllAttrTypes(), cf.numColumns, cf.getAllAttrSizes(), it.get(0), sortmem, true);
            ArrayList<Tuple> sortTuples = new ArrayList<Tuple>();
            Tuple result;
            while (true) {
                result = it1.get_next();
                if (result == null) {
                    break;
                } else {
                    System.out.println("Adding");
                    sortTuples.add(result);
                }
                result.print(opAttr);
                if(cnt >= 2) {
                    System.out.println("PREV");
                    sortTuples.get(cnt - 1).print(opAttr);
                    sortTuples.get(cnt - 2).print(opAttr);
                }
                cnt++;
                result = null;
                if(result == null) {
                    System.out.println("null");
                }
            }
            Tuple temp;
            for (int i = 0; i < sortTuples.size(); i++) {
                temp = sortTuples.get(i);
                System.out.println("added tuple no: " + (i + 1));
                temp.print(opAttr);
            }
            System.out.print("SIZE:: ");
            System.out.println(sortTuples.size());

//            Boolean deleted = true;
//            while (deleted) {
//                deleted = it.get(1).delete_next();
//
//                if (deleted == false) {
//                    break;
//                }
//                cnt++;
//            }
            it.get(1).close();
            it.get(0).close();
//            cf.purgeAllDeletedTuples();
//            System.out.println("deleting done successfully!!!!");
//            Tuple temp;
//            for (int i = 0; i < sortTuples.size(); i++) {
////                cf.insertTuple(sortTuples.get(i).getTupleByteArray());
//                System.out.println("added tuple no: " + (i + 1));
//                temp = sortTuples.get(i);
//                temp.print(opAttr);
//            }
            System.out.println("all tuples added");

            System.out.println();
            System.out.println(cnt + " tuples selected");
            System.out.println();
            cf.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            it.get(0).close();
            it.get(1).close();
            it1.close();
        }
    }
}
