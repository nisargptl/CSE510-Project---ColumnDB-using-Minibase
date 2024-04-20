package iterator;

import global.AttrType;
import heap.*;
import index.IndexException;

import java.io.IOException;

public class ColumnarNestedLoopJoins extends Iterator {
    private final AttrType[] _in1;
    private final AttrType[] _in2;
    private final Iterator _am1;
    private Iterator _am2;
    private Tuple innerTuple;
    private final Tuple Jtuple;
    private Tuple outerTuple;
    private final CondExpr[] RightFilter;
    private final CondExpr[] OutputFilter;
    private final short[] InnertTargets;
    private final FldSpec[] perm_mat;
    private final FldSpec[] i_proj;
    private final String cfName;
    private boolean done, getfromouter;

    public ColumnarNestedLoopJoins(AttrType[] in1, AttrType[] in2, Iterator am1, String relName, CondExpr[] outputFilter, CondExpr[] innerFilter, short[] innerTargets, FldSpec[] inner_Proj, FldSpec[] proj_list, Tuple proj_t) {
        _in1 = in1;
        _in2 = in2;
        _am1 = am1;
        _am2 = null;
        innerTuple = new Tuple();
        Jtuple = proj_t;
        RightFilter = innerFilter;
        OutputFilter = outputFilter;
        InnertTargets = innerTargets;
        done = false;
        getfromouter = true;
        perm_mat = proj_list;
        cfName = relName;
        i_proj = inner_Proj;
    }

    public Tuple get_next() throws Exception {
        if (done)
            return null;
        do {
            if (getfromouter) {
                getfromouter = false;
                if (_am2 != null) {
                    _am2 = null;
                }
                try {
                    _am2 = new ColumnarFileScan(cfName, i_proj, InnertTargets, RightFilter);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if ((outerTuple = _am1.get_next()) == null) {
                    done = true;
                    return null;
                }
            }

            while ((innerTuple = _am2.get_next()) != null) {
                if (PredEval.Eval(OutputFilter, outerTuple, innerTuple, _in1, _in2)) {
                    Projection.Join(outerTuple, _in1, innerTuple, _in2, Jtuple, perm_mat, perm_mat.length);
                    return Jtuple;
                }
            }
            getfromouter = true;
        } while (true);
    }

    public void close() throws JoinsException, IOException, IndexException {
        if (!closeFlag) {
            try {
                if (_am2 != null) {
                    _am2.close();
                }
                _am1.close();
            } catch (Exception e) {
                throw new JoinsException(e, "Error: Failed closing iterators in ColumnarNestedLoopJoin.java!");
            }
            closeFlag = true;
        }
    }
}