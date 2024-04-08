package tests;

import columnar.ColumnarBitmapEquiJoins;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

public class BitmapEquiJoin {
    public static void main(String[] args) throws Exception {
        if (args.length != 8) {
            System.out.println("Incorrect number of arguments.");
            System.out.println("Usage: BitmapEquiJoin <COLUMNDB> <OUTERFILE> <INNERFILE> <OUTERCONST> <INNERCONST> <EQUICONST> <TARGETCOLUMNS> <NUMBUF>");
            return;
        }

        String columnDB = args[0];
        String outerColumnarFile = args[1];
        String innerColumnarFile = args[2];
        String rawOuterConstraint = args[3];
        String rawInnerConstraint = args[4];
        String rawEquijoinConstraint = args[5];
        String[] targetColumns = args[6].split(",");
        Integer bufferSize = Integer.parseInt(args[7]);

        String dbpath = "/tmp/" + columnDB;
        SystemDefs sysdef = new SystemDefs(dbpath, 1000, bufferSize, "Clock");

        runInterface(outerColumnarFile, innerColumnarFile, rawOuterConstraint, rawInnerConstraint, rawEquijoinConstraint, targetColumns);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String outerColumnarFile, String innerColumnarFile,
                                     String rawOuterConstraint, String rawInnerConstraint,
                                     String rawEquijoinConstraint, String[] targetColumns) throws Exception {
        // Implementation remains as it interacts primarily with Columnarfile and CondExpr processing,
        // which is not directly affected by the BitMapFile adjustments.
        Columnarfile outer = new Columnarfile(outerColumnarFile);
        Columnarfile inner = new Columnarfile(innerColumnarFile);

        CondExpr[] innerColumnarConstraint = InterfaceUtils.processRawConditionExpression(rawInnerConstraint, inner);
        CondExpr[] outerColumnarConstraint = InterfaceUtils.processRawConditionExpression(rawOuterConstraint, outer);
        CondExpr[] equiJoinConstraint = InterfaceUtils.processEquiJoinConditionExpression(rawEquijoinConstraint, inner, outer);

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

        // Call the equijoin interface
        /*
        *
        * AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            AttrType[] in2,
            int len_in2,
            short[] t2_str_sizes,
            int amt_of_mem,
            String leftColumnarFileName,
            int leftJoinField,
            String rightColumnarFileName,
            int rightJoinField,
            FldSpec[] proj_list,
            int n_out_flds,
            CondExpr[] joinExp,
            CondExpr[] innerExp,
            CondExpr[] outerExp
        * */

        ColumnarBitmapEquiJoins columnarBitmapEquiJoins = new ColumnarBitmapEquiJoins(outer.getAttributes(),
                outer.getnumColumns(), outer.getAttrSizes(),
                inner.getAttributes(),
                inner.getnumColumns(),
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
