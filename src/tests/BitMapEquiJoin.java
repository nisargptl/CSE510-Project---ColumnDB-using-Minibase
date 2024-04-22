package tests;

import btree.IndexFileScan;
import columnar.ColumnarBitmapEquiJoins;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import index.IndexScan;
import index.IndexUtils;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

public class BitMapEquiJoin {

    public static void main(String[] args) throws Exception {
        String columnDB = args[0];
        String outerColumnarFile = args[1];
        String innerColumnarFile = args[2];
        String rawOuterConstraint = args[3];
        String rawInnerConstraint = args[4];
        String rawEquijoinConstraint = args[5];
        String[] targetColumns = args[6].split(",");
        Integer bufferSize = Integer.parseInt(args[7]);

        String dbpath = OperationUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        runInterface(outerColumnarFile, innerColumnarFile, rawOuterConstraint, rawInnerConstraint, rawEquijoinConstraint, targetColumns);

        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    public static String[] getColumnNames(Columnarfile columnarFile) throws Exception {
        int numColumns = columnarFile.getOffset();
        String[] columnNames = new String[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columnNames[i] = columnarFile.getAttrtypeforcolumn(i).toString(); // Ensure getAttrName or similar method exists to get the name
        }
        return columnNames;
    }

    private static void runInterface(String outerColumnarFile, String innerColumnarFile, String rawOuterConstraint, String rawInnerConstraint, String rawEquijoinConstraint, String[] targetColumns) throws Exception {
        Columnarfile outer = new Columnarfile(outerColumnarFile);
        Columnarfile inner = new Columnarfile(innerColumnarFile);

        CondExpr[] innerColumnarConstraint = OperationUtils.processRawConditionExpression(rawInnerConstraint, targetColumns);
        CondExpr[] outerColumnarConstraint = OperationUtils.processRawConditionExpression(rawOuterConstraint, targetColumns);
        CondExpr[] equiJoinConstraint = OperationUtils.processEquiJoinConditionExpression(rawEquijoinConstraint, inner, outer);

        AttrType[] opAttr = new AttrType[targetColumns.length];
        FldSpec[] projectionList = new FldSpec[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++) {
            String attribute = targetColumns[i].split("\\.")[1];
            String relationName = targetColumns[i].split("\\.")[0];
            if (relationName.equals(outerColumnarFile)) {
                projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), outer.getAttributePosition(attribute) + 1);
                opAttr[i] = new AttrType(outer.getAttrtypeforcolumn(outer.getAttributePosition(attribute)).attrType);
            } else {
                projectionList[i] = new FldSpec(new RelSpec(RelSpec.innerRel), inner.getAttributePosition(attribute) + 1);
                opAttr[i] = new AttrType(inner.getAttrtypeforcolumn(inner.getAttributePosition(attribute)).attrType);
            }
        }

        ColumnarBitmapEquiJoins columnarBitmapEquiJoins = new ColumnarBitmapEquiJoins(outer.getAttributes(),
                outer.getOffset(), outer.getAttrSizes(),
                inner.getAttributes(),
                inner.getOffset(),
                inner.getAttrSizes(),
                2,
                outerColumnarFile,
                -1,
                innerColumnarFile,
                -1,
                projectionList,
                targetColumns.length,
                equiJoinConstraint,
                innerColumnarConstraint, outerColumnarConstraint, opAttr);

        outer.close();
        inner.close();
    }
}
