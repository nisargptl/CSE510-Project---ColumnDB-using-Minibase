package columnar;

import bitmap.BitMapFile;
import global.*;
import heap.*;
import iterator.*;

import java.util.*;

public class ColumnarBitmapEquiJoins {
    private final Columnarfile leftColumnarFile;
    private final Columnarfile rightColumnarFile;

    public ColumnarBitmapEquiJoinsII(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            AttrType[] in2, int len_in2, short[] t2_str_sizes,
            int amt_of_mem, String leftColumnarFileName, int leftJoinField,
            String rightColumnarFileName, int rightJoinField,
            FldSpec[] proj_list, int n_out_flds, CondExpr[] joinExp,
            CondExpr[] innerExp, CondExpr[] outerExp, AttrType[] opAttr) throws Exception {

        leftColumnarFile = new Columnarfile(leftColumnarFileName);
        rightColumnarFile = new Columnarfile(rightColumnarFileName);

        // Simulated logic to process joins and constraints
        executeJoin(joinExp, innerExp, outerExp, proj_list, n_out_flds, opAttr);
    }

    private void executeJoin(CondExpr[] joinExp, CondExpr[] innerExp, CondExpr[] outerExp,
                             FldSpec[] proj_list, int n_out_flds, AttrType[] opAttr) throws Exception {

        List<BitSet> leftBitSets = new ArrayList<>();
        List<BitSet> rightBitSets = new ArrayList<>();

        // Iterate over join conditions and create/retrieve bitmap files for each join condition
        for (CondExpr cond : joinExp) {
            // Assume bitmap names are constructed based on field and value.
            // Here, retrieve or create bitmap indexes for specified fields and conditions
            String leftBitmapFileName = constructBitmapFileName(leftColumnarFile, leftJoinField, cond);
            String rightBitmapFileName = constructBitmapFileName(rightColumnarFile, rightJoinField, cond);

            BitMapFile leftBMF = new BitMapFile(leftBitmapFileName);
            BitMapFile rightBMF = new BitMapFile(rightBitmapFileName);

            // Fetch the bitmaps for each condition and store them
            leftBitSets.add(leftBMF.getBitMap());
            rightBitSets.add(rightBMF.getBitMap());
        }

        // Compute the logical AND of all bitmap vectors retrieved from the bitmap files
        BitSet leftResultantBitmap = leftBitSets.get(0);
        BitSet rightResultantBitmap = rightBitSets.get(0);
        for (int i = 1; i < leftBitSets.size(); i++) {
            leftResultantBitmap.and(leftBitSets.get(i));
            rightResultantBitmap.and(rightBitSets.get(i));
        }

        // Use the resultant bitmaps to find matching tuples
        Tuple joinedTuple = new Tuple();
        for (int i = leftResultantBitmap.nextSetBit(0); i >= 0; i = leftResultantBitmap.nextSetBit(i+1)) {
            if (rightResultantBitmap.get(i)) {
                Tuple leftTuple = leftColumnarFile.getTuple(i);
                Tuple rightTuple = rightColumnarFile.getTuple(i);

                // Project the tuples based on the projection list
                TupleUtils.setup_op_tuple(joinedTuple, opAttr, in1, len_in1, in2, len_in2, t1_str_sizes, t2_str_sizes, proj_list, n_out_flds);
                Projection.Join(leftTuple, in1, rightTuple, in2, joinedTuple, proj_list, n_out_flds);

                // Handle the joined tuple as needed (e.g., output or further processing)
            }
        }
    }

    private String constructBitmapFileName(Columnarfile file, int fieldNo, CondExpr cond) {
        // Construct a file name based on columnar file metadata and condition specifics
        String columnName = file.getColumnName(fieldNo);
        String value = cond.operand2.string;
        return file.getFileName() + "_" + columnName + "_" + value + ".bm";
    }
}
