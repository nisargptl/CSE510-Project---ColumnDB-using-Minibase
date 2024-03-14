package index;

// import columnar.Columnarfile;

// public class ColumnIndexScan {

//     public ColumnIndexScan(Columnarfile cf, int columnNo, CondExpr[] selects, boolean indexOnly) {
//         _selects = selects;
//         index_only = indexOnly;
//         try {

//             columnarfile = cf;
//             indName = columnarfile.getBTName(columnNo);
//         } catch (Exception e) {
//             e.printStackTrace();
//             return;
//         }

//         try {
//             btIndFile = new BTreeFile(indName);
//         } catch (Exception e) {
//             throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
//         }

//         try {
//             btIndScan = IndexUtils.BTree_scan(_selects, btIndFile);
//         } catch (Exception e) {
//             throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
//         }
//     }
// }

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;
import java.util.Arrays;

import bitmap.*;
import columnar.*;

/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. It will also perform selections and projections.
 * information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class ColumnIndexScan extends Iterator {

    public FldSpec[] perm_mat;
    private final IndexFile indFile;
    private final IndexFileScan indScan;
    private final CondExpr[] _selects;
    private int _noInFlds;
    private int _noOutFlds;
    private final Heapfile f;
    private Tuple tuple1;
    private Tuple Jtuple;
    private int t1_size;
    private int _fldNum;
    private final boolean index_only;
    private String _colFileName;
    private final String _relName;
    private final String _indName;
    private BitMapFile bitMapFile;
    private final AttrType _type;
    private final short _s_sizes;
    private int[] _outputColumnsIndexes;
    private Scan bitMapScan;
    private final Columnarfile columnarfile;
    private IndexType index;

    /**
     * class constructor. set up the index scan.
     *
     * @param index     type of the index (B_Index, Hash, BitMap)
     * @param relName   name of the input relation
     * @param indName   name of the input index
     * @param type      type of the col
     * @param str_sizes string sizes (for attributes that are string)
     * @param selects   conditions to apply, first one is primary
     * @param indexOnly whether the answer requires only the key or the tuple
     * @exception IndexException            error from the lower layer
     * @exception InvalidTypeException      tuple type not valid
     * @exception InvalidTupleSizeException tuple size not valid
     * @exception UnknownIndexTypeException index type unknown
     * @exception IOException               from the lower layer
     */
    public ColumnIndexScan(
            IndexType index,
            final String relName,
            final String indName,
            AttrType type,
            short str_sizes,
            CondExpr[] selects,
            final boolean indexOnly)
            throws IndexException,
            InvalidTypeException,
            InvalidTupleSizeException,
            UnknownIndexTypeException,
            IOException, HFDiskMgrException, HFException, HFBufMgrException, Exception {
        _type = type;
        _s_sizes = str_sizes;
        index_only = indexOnly;
        _selects = selects;
        _relName = relName;
        _indName = indName;
        columnarfile = new Columnarfile(relName);

        // AttrType[] Jtypes = new AttrType[noOutFlds];
        // short[] ts_sizes;
        // Jtuple = new Tuple();

        // try {
        // ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds,
        // str_sizes, outFlds, noOutFlds);
        // } catch (TupleUtilsException e) {
        // throw new IndexException(e, "ColumnIndexScan.java: TupleUtilsException caught
        // from TupleUtils.setup_op_tuple()");
        // } catch (InvalidRelation e) {
        // throw new IndexException(e, "ColumnIndexScan.java: InvalidRelation caught
        // from TupleUtils.setup_op_tuple()");
        // }

        // perm_mat = outFlds;
        // _noOutFlds = noOutFlds;
        // tuple1 = new Tuple();
        // try {
        // tuple1.setHdr((short) noInFlds, types, str_sizes);
        // } catch (Exception e) {
        // throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
        // }

        // t1_size = tuple1.size();
        // index_only = indexOnly; // added by bingjie miao

        try {
            f = new Heapfile(relName);
        } catch (Exception e) {
            throw new IndexException(e, "ColumnIndexScan.java: Heap file not created");
        }

        switch (index.indexType) {
            // linear hashing is not yet implemented
            case IndexType.B_Index:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value
                try {
                    indFile = new BTreeFile(indName);
                } catch (Exception e) {
                    throw new IndexException(e,
                            "ColumnIndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    indScan = IndexUtils.BTree_scan(selects, indFile);
                } catch (Exception e) {
                    throw new IndexException(e,
                            "ColumnIndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;

            case IndexType.BitMapIndex:
                try {
                    bitMapFile = new BitMapFile(relName);
                } catch (GetFileEntryException e) {
                    throw new IndexException(e,
                            "ColumnIndexScan.java: GetFileEntryException caught from BitMapFile constructor");
                }
                // todo: implement bitmap index case here (needs BitMapFileScan or implement
                // something similar here)
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far"); // todo: edit this

        }

    }

    /**
     * returns the next tuple.
     * if <code>index_only</code>, only returns the key value
     * (as the first field in a tuple)
     * otherwise, retrieve the tuple and returns the whole tuple
     *
     * @return the tuple
     * @exception IndexException          error from the lower layer
     * @exception UnknownKeyTypeException key type unknown
     * @exception IOException             from the lower layer
     */

    public Tuple get_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException {
        if (index.indexType == IndexType.B_Index) {
            return get_btree_next();
        } else {
            return null;// get_bm_next();
        }
    }

    public Tuple get_btree_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException {
        RID rid;
        int unused;
        KeyDataEntry nextentry = null;

        try {
            nextentry = indScan.get_next();
        } catch (Exception e) {
            throw new IndexException(e, "ColumnIndexScan.java: BTree error");
        }

        while (nextentry != null) {
            if (index_only) {
                // only need to return the key

                AttrType[] attrType = new AttrType[1];
                short[] s_sizes = new short[0];

                if (_type.attrType == AttrType.attrInteger) {
                    attrType[0] = new AttrType(AttrType.attrInteger);
                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey().intValue());
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
                    }
                } else if (_type.attrType == AttrType.attrString) {

                    attrType[0] = new AttrType(AttrType.attrString);
                    // calculate string size of _fldNum
                    int count = 0;
                    for (int i = 0; i < _fldNum; i++) {
                        if (_type.attrType == AttrType.attrString)
                            count++;
                    }
                    s_sizes[0] = _s_sizes;

                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
                    } catch (Exception e) {
                        throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
                    }
                } else {
                    // attrReal not supported for now
                    throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
                }
                return Jtuple;
            }
            // todo: look into it

            // not index_only, need to return the whole tuple
            // rid = ((LeafData) nextentry.data).getData();
            // try {
            // tuple1 = f.getRecord(rid);
            // } catch (Exception e) {
            // throw new IndexException(e, "ColumnIndexScan.java: getRecord failed");
            // }

            // try {
            // tuple1.setHdr((short) _noInFlds, _type, _s_sizes);
            // } catch (Exception e) {
            // throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
            // }

            // boolean eval;
            // try {
            // eval = PredEval.Eval(_selects, tuple1, null, _type, null);
            // } catch (Exception e) {
            // throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
            // }

            // if (eval) {
            // // need projection.java
            // try {
            // Projection.Project(tuple1, _type, Jtuple, perm_mat, _noOutFlds);
            // } catch (Exception e) {
            // throw new IndexException(e, "ColumnIndexScan.java: Heapfile error");
            // }

            // return Jtuple;
            // }

            // try {
            // nextentry = indScan.get_next();
            // } catch (Exception e) {
            // throw new IndexException(e, "ColumnIndexScan.java: BTree error");
            // }

            // Retrieve the Record ID (RID) from the next entry in the index scan
            // rid = ((LeafData) nextentry.data).getData();

            // // Print the key of the next entry, depending on its type
            // if (nextentry.key instanceof IntegerKey) {
            // System.out.println("Record Match found Key " + ((IntegerKey)
            // nextentry.key).getKey().intValue());
            // } else {
            // System.out.println("Record Match found Key " + ((StringKey)
            // nextentry.key).getKey().toString());
            // }

            // // Print the position of the record
            // System.out.println("Record Match found at Position " + (rid.position + 1));

            // // Get the number of output columns
            // int numOfOutputColumns = _outputColumnsIndexes.length;

            // // Initialize the Columnarfile and Heapfile
            // Columnarfile cf = new Columnarfile(_colFileName);
            // Heapfile hf = new Heapfile(_relName);

            // // Get the position of the record in the heap file
            // int position = getPositionFromRID(rid, hf);

            // // Get the attribute types of the columns in the columnar file
            // AttrType[] attrType = cf.getType();

            // // Create arrays to store the required attribute types and string sizes for
            // the
            // // output columns
            // AttrType[] reqAttrType = new AttrType[numOfOutputColumns];
            // short[] s_sizes = new short[numOfOutputColumns];

            // // Populate the reqAttrType and s_sizes arrays
            // int j = 0;
            // for (int i = 0; i < numOfOutputColumns; i++) {
            // reqAttrType[i] = attrType[_outputColumnsIndexes[i] - 1];
            // if (reqAttrType[i].attrType == AttrType.attrString) {
            // s_sizes[j] = _s_sizes[_outputColumnsIndexes[i] - 1];
            // j++;
            // }
            // }

            // // Copy the relevant part of s_sizes to strSizes
            // short[] strSizes = Arrays.copyOfRange(s_sizes, 0, j);

            // // Create a new Tuple and set its header
            // Tuple tuple = new Tuple();
            // try {
            // tuple.setHdr((short) numOfOutputColumns, reqAttrType, strSizes);
            // } catch (InvalidTypeException e) {
            // e.printStackTrace();
            // }

            // // Populate the new tuple with data from the corresponding columns in the
            // // columnar file
            // for (int i = 0; i < numOfOutputColumns; i++) {
            // int indexNumber = _outputColumnsIndexes[i];
            // Heapfile heapfile = cf.getColumnFiles()[indexNumber - 1];
            // Tuple tupleTemp = Util.getTupleFromPosition(position, heapfile);
            // tupleTemp.initHeaders();
            // if (attrType[indexNumber - 1].attrType == AttrType.attrString) {
            // tuple.setStrFld(i + 1, tupleTemp.getStrFld(1));
            // } else if (attrType[indexNumber - 1].attrType == AttrType.attrInteger) {
            // tuple.setIntFld(i + 1, tupleTemp.getIntFld(1));
            // } else if (attrType[indexNumber - 1].attrType == AttrType.attrReal) {
            // tuple.setFloFld(i + 1, tupleTemp.getFloFld(1));
            // }
            // }

            // // Return the new tuple
            // return tuple;
        }

        return null;
    }

    /**
     * Cleaning up the index scan, does not remove either the original
     * relation or the index from the database.
     *
     * @exception IndexException error from the lower layer
     * @exception IOException    from the lower layer
     */
    public void close() throws IOException, IndexException {
        if (!closeFlag) {
            if (indScan instanceof BTFileScan) {
                try {
                    ((BTFileScan) indScan).DestroyBTreeFileScan();
                } catch (Exception e) {
                    throw new IndexException(e, "BTree error in destroying index scan.");
                }
            }

            closeFlag = true;
        }
    }

}