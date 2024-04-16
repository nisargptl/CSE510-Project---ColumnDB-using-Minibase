package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    private List<BitmapFileScan> bitMapScans = new ArrayList<>();
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
            int columnNo,
            IndexType index,
            final String relName,
            final String indName,
            AttrType type,
            short str_sizes,
            CondExpr[] selects,
            final boolean indexOnly)
            throws Exception {
        _type = type;
        _s_sizes = str_sizes;
        index_only = indexOnly;
        _selects = selects;
        _relName = relName;
        _indName = indName;
        this.index = index;
        columnarfile = new Columnarfile(relName);

        Jtuple = new Tuple();

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
                    System.out.println("");
                    System.out.println(indName);
                    System.out.println("");
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
                    indFile = new BitMapFile(relName);
                } catch (GetFileEntryException e) {
                    throw new IndexException(e,
                            "ColumnIndexScan.java: GetFileEntryException caught from BitMapFile constructor");
                }
                // todo: implement bitmap index case here (needs BitMapFileScan or implements something similar here)
                indScan = IndexUtils.Bitmap_scan(columnarfile, columnNo, selects, index_only);
                System.out.println(indScan.get_next().data);
                break;
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
     * @throws ScanIteratorException
     * @throws FieldNumberOutOfBoundException
     * @throws InvalidTupleSizeException
     * @throws InvalidTypeException
     */

    public Tuple get_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException,
            ScanIteratorException {
        if (index.indexType == IndexType.B_Index) {
            return get_btree_next();
        } else {
            // return null;
            return get_bm_next();
        }
    }

    public Tuple get_btree_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException {
        // RID rid;
        // int unused;
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

        }

        return null;
    }

    public Tuple get_bm_next()
            throws InvalidTypeException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException,
            ScanIteratorException {
        int position = -1;

        for (BitmapFileScan scan : bitMapScans) {
            position = scan.get_next();
            // Handle position found

            if (position != -1) {
                break;
            }
            AttrType[] attrType = new AttrType[1];
            short[] s_sizes = new short[0];

            attrType[0] = new AttrType(AttrType.attrInteger);
            Jtuple.setHdr((short) 1, attrType, s_sizes);
            Jtuple.setIntFld(1, position);

            // return Jtuple;
        }

        if (position == -1) {
            return null;
        }
        return Jtuple;

        // while (position != -1) {
        // // If the position is -1, there are no more entries to scan
        // // Tuple resultTuple = new Tuple();
        // AttrType[] attrType = new AttrType[1];
        // short[] s_sizes = new short[0];

        // attrType[0] = new AttrType(AttrType.attrInteger);
        // Jtuple.setHdr((short) 1, attrType, s_sizes);
        // Jtuple.setIntFld(1, position);

        // return Jtuple;
        // }
        // return null;
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