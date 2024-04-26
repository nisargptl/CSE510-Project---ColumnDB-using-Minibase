package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
            boolean indexOnly,
            Map<Integer, CondExpr[]> condExprMap) throws Exception {
        this.types = types;
        this.str_sizes = str_sizes;
        this.noInFlds = noInFlds;
        this.noOutFlds = noOutFlds;
        this.outFlds = outFlds;
        this.selects = selects;
        this.indexOnly = indexOnly;

        // Assuming columnarfile provides access to the columnar storage.
        this.columnarfile = new Columnarfile(relName);
        int c = 0;
        int i = 0;
        indexScans = new ColumnIndexScan[condExprMap.size()];

        for (Entry<Integer, CondExpr[]> entry : condExprMap.entrySet()) {
            Integer column = entry.getKey(); // This is the column index
            CondExpr[] conditions = entry.getValue(); // This is the list of conditions for the column

            if (types[column].attrType == AttrType.attrString) {
                indexScans[i] = new ColumnIndexScan(column, index[i], relName, indName[i],
                        types[fldNum[i]],
                        str_sizes[i - c], conditions, indexOnly);
            } else {
                c += 1;
                indexScans[i] = new ColumnIndexScan(column, index[i], relName, indName[i],
                        types[fldNum[i]], (short) 0,
                        conditions, indexOnly);
            }

            i++;
        }

        // Prepare a tuple template for projections.
        try {
            Jtuple = new Tuple();
            AttrType[] Jtypes = new AttrType[noOutFlds];
            // System.out.println("num out: " + noOutFlds);
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
        // Tuple resultTuple = null;
        Tuple resultTuple = new Tuple(Jtuple);
        int resultFldNo = 0;
        boolean isResultTuplePopulated = false;

        for (ColumnIndexScan scan : indexScans) {
            Tuple tempTuple = scan.get_next();
            if (tempTuple == null)
                continue;

            for (int i = 0; i < tempTuple.fldCnt; i++) {
                if (scan.getScanAttrType().attrType == AttrType.attrInteger) {
                    resultTuple.setIntFld(resultFldNo + 1, tempTuple.getIntFld(i + 1));
                    resultFldNo++;
                } else if (scan.getScanAttrType().attrType == AttrType.attrString) {
                    resultTuple.setStrFld(resultFldNo + 1, tempTuple.getStrFld(i + 1));
                    resultFldNo++;
                }
                resultFldNo++;

            }
            isResultTuplePopulated = true;
            // }
        }

        if (isResultTuplePopulated == false)
            return null;

        Tuple outputTuple = new Tuple(Jtuple.size());
        outputTuple.setHdr((short) noOutFlds, types, str_sizes);

        // Applying projection
        Projection.Project(resultTuple, types, outputTuple, outFlds, noOutFlds);
        // Projection.Project(resultTuple, types, outputTuple, outFlds, 1);

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