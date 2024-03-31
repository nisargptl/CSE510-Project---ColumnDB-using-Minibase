package iterator;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import global.TID;
import columnar.TupleScan;
import global.*;
import heap.*;

import java.io.IOException;

public class ColumnarFileScan extends Iterator{

    private final Columnarfile columnarfile;
    private final TupleScan scan;
    private Tuple     tuple1;
    private final Tuple    Jtuple;
    private final CondExpr[]  OutputFilter;
    public FldSpec[] perm_mat;
    Sort deletedTuples;
    private int currDeletePos = -1;
    private AttrType[] targetAttrTypes = null;

    /**
     * constructor
     *
     * @param file_name  columnarfile to be opened
     * @param proj_list  shows what input fields go where in the output tuple
     * @param outFilter  select expressions
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
    public ColumnarFileScan(java.lang.String file_name,
                            FldSpec[] proj_list,
                            short[] targetedCols,
                            CondExpr[] outFilter) throws FileScanException, TupleUtilsException, IOException, InvalidRelation {

        OutputFilter = outFilter;
        perm_mat = proj_list;

        try {
            columnarfile = new Columnarfile(file_name);
            targetAttrTypes = ColumnarScanUtils.getTargetColumnAttributeTypes(columnarfile, targetedCols);
            Jtuple = ColumnarScanUtils.getProjectionTuple(columnarfile, perm_mat, targetedCols);
            scan = columnarfile.openTupleScan(targetedCols);
        }
        catch(Exception e){
            throw new FileScanException(e, "openScan() failed");
        }
    }

    /**
     *@return shows what input fields go where in the output tuple
     */
    public FldSpec[] show()
    {
        return perm_mat;
    }

    /**
     *@return the result tuple
     *@exception JoinsException some join exception
     *@exception IOException I/O errors
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception PredEvalException exception from PredEval class
     *@exception UnknowAttrType attribute type unknown
     *@exception FieldNumberOutOfBoundException array out of bounds
     *@exception WrongPermat exception for wrong FldSpec argument
     */
    public Tuple get_next()
            throws Exception {

        int position = getNextPosition();

        if (position < 0)
            return null;

        Projection.Project(tuple1, targetAttrTypes, Jtuple, perm_mat, perm_mat.length);
        return Jtuple;
    }

//    public boolean delete_next()
//            throws Exception {
//
//        int position = getNextPosition();
//
//        if (position < 0)
//            return false;
//
//        return columnarfile.markTupleDeleted(position);
//    }

    private int getNextPosition()
            throws Exception {
        TID tid = new TID();

        while(true) {
            if((tuple1 =  scan.getNext(tid)) == null) {
                return -1;
            }

            int position = tid.getPosition();
            if(deletedTuples != null && position > currDeletePos){
                while (true){
                    Tuple dtuple = deletedTuples.get_next();
                    if(dtuple == null)
                        break;
                    currDeletePos = dtuple.getIntFld(1);
                    if(currDeletePos >= position)
                        break;
                }
            }
            if(position == currDeletePos){
                Tuple dtuple = deletedTuples.get_next();
                if(dtuple == null)
                    break;
                currDeletePos = dtuple.getIntFld(1);
                continue;
            }

            //tuple1.setHdr(in1_len, _in1, s_sizes);
            if (PredEval.Eval(OutputFilter, tuple1, null, targetAttrTypes, null)){
                return position;
            }
        }
        return -1;
    }

    /**
     *implement the abstract method close() from super class Iterator
     *to finish cleaning up
     */
    public void close() throws IOException, SortException {

        if (!closeFlag) {
            scan.closetuplescan();
            if(deletedTuples != null)
                deletedTuples.close();
            closeFlag = true;
        }
    }
}

