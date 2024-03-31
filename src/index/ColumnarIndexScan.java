package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;
import java.util.Arrays;

import columnar.*;

public class ColumnarIndexScan extends Iterator {
    private final ColumnIndexScan[] indexScans;
    private final AttrType[] types;
    private final short[] str_sizes;
    private final int noInFlds;
    private final int noOutFlds;
    private final FldSpec[] outFlds;
    private final CondExpr[] selects;
    private final boolean indexOnly;

    private final Tuple Jtuple; // To hold projected tuples.
    private final Columnarfile columnarfile;

    /**
     * ColumnarIndexScan constructor.
     *
     * @throws Exception Various exceptions can occur inside database operations.
     */
    public ColumnarIndexScan(
            String relName,
            int[] fldNum,
            IndexType[] index,
            String[] indName,
            AttrType[] types,
            short[] str_sizes,
            int noInFlds,
            int noOutFlds,
            FldSpec[] outFlds,
            CondExpr[] selects,
            boolean indexOnly) throws Exception {
        this.types = types;
        this.str_sizes = str_sizes;
        this.noInFlds = noInFlds;
        this.noOutFlds = noOutFlds;
        this.outFlds = outFlds;
        this.selects = selects;
        this.indexOnly = indexOnly;

        // Assuming columnarfile provides access to the columnar storage.
        this.columnarfile = new Columnarfile(relName);

        indexScans = new ColumnIndexScan[fldNum.length];
        for (int i = 0; i < fldNum.length; i++) {
            indexScans[i] = new ColumnIndexScan(index[i], relName, indName[i], types[fldNum[i] - 1],
                    str_sizes[i], selects, indexOnly);
        }

        // Prepare a tuple template for projections.
        try {
            Jtuple = new Tuple();
            AttrType[] Jtypes = new AttrType[noOutFlds];
            short[] ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds,
                    new AttrType[0], 0,
                    str_sizes, new short[0],
                    outFlds, noOutFlds);
        } catch (TupleUtilsException e) {
            throw new Exception("Exception occurred during ColumnarIndexScan setup.", e);
        }
    }

    @Override
    public Tuple get_next() throws Exception {
        int minPosition = Integer.MAX_VALUE;
        Tuple resultTuple = null;

        for (ColumnIndexScan scan : indexScans) {
            Tuple tempTuple = scan.get_next();

            if (tempTuple == null)
                continue;
            int position = tempTuple.getIntFld(1); // Assuming the position is stored in the first field.

            if (position < minPosition) {
                minPosition = position;
                resultTuple = tempTuple;
            }
        }

        if (resultTuple == null)
            return null;

        Tuple outputTuple = new Tuple(Jtuple.size());
        outputTuple.setHdr((short) noOutFlds, types, str_sizes);

        // Applying projection
        Projection.Project(resultTuple, types, outputTuple, outFlds, noOutFlds);

        // if (PredEval.Eval(selects, resultTuple, null, types, null)) {
        // Projection.Project(resultTuple, types, outputTuple, outFlds, noOutFlds);
        // return outputTuple;
        // }

        return outputTuple;
    }

    @Override
    public void close() throws IOException, JoinsException, IndexException, SortException {
        for (ColumnIndexScan scan : indexScans) {
            scan.close();
        }
    }
}