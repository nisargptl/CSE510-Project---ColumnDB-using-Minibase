package index;

import bitmap.BitMapFile;
import bitmap.BitmapFileScan;
import columnar.Columnarfile;
import global.*;
import btree.*;
import heap.Tuple;
import iterator.*;
import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * IndexUtils class opens an index scan based on selection conditions.
 * Currently only BTree_scan is supported
 */
public class IndexUtils {
	static Columnarfile columnarfile;

	static CondExpr[] _selects;

	/**
	 * BTree_scan opens a BTree scan based on selection conditions
	 * 
	 * @param selects conditions to apply
	 * @param indFile the index (BTree) file
	 * @return an instance of IndexFileScan (BTreeFileScan)
	 * @exception IOException               from lower layer
	 * @exception UnknownKeyTypeException   only int and string keys are supported
	 * @exception InvalidSelectionException selection conditions (selects) not valid
	 * @exception KeyNotMatchException      Keys do not match
	 * @exception UnpinPageException        unpin page failed
	 * @exception PinPageException          pin page failed
	 * @exception IteratorException         iterator exception
	 * @exception ConstructPageException    failed to construct a header page
	 */
	public static IndexFileScan BTree_scan(CondExpr[] selects, IndexFile indFile)
			throws IOException,
			UnknownKeyTypeException,
			InvalidSelectionException,
			KeyNotMatchException,
			UnpinPageException,
			PinPageException,
			IteratorException,
			ConstructPageException {
		IndexFileScan indScan;

		if (selects == null || selects[0] == null) {
			indScan = ((BTreeFile) indFile).new_scan(null, null);
			return indScan;
		}

		if (selects[1] == null) {
			if (selects[0].type1.attrType != AttrType.attrSymbol && selects[0].type2.attrType != AttrType.attrSymbol) {
				throw new InvalidSelectionException("IndexUtils.java: Invalid selection condition");
			}

			KeyClass key;

			// symbol = value
			if (selects[0].op.attrOperator == AttrOperator.aopEQ) {
				if (selects[0].type1.attrType != AttrType.attrSymbol) {
					key = getValue(selects[0], selects[0].type1, 1);
					indScan = ((BTreeFile) indFile).new_scan(key, key);
				} else {
					key = getValue(selects[0], selects[0].type2, 2);
					indScan = ((BTreeFile) indFile).new_scan(key, key);
				}
				return indScan;
			}

			// symbol < value
			if (selects[0].op.attrOperator == AttrOperator.aopLT) {
				if (selects[0].type1.attrType != AttrType.attrSymbol) {
					selects[0].operand1.integer--;
					key = getValue(selects[0], selects[0].type1, 1);
					indScan = ((BTreeFile) indFile).new_scan(null, key);
				} else {
					selects[0].operand2.integer--;
					key = getValue(selects[0], selects[0].type2, 2);
					indScan = ((BTreeFile) indFile).new_scan(null, key);
				}
				return indScan;
			}

			// or symbol <= value
			if (selects[0].op.attrOperator == AttrOperator.aopLE) {
				if (selects[0].type1.attrType != AttrType.attrSymbol) {
					key = getValue(selects[0], selects[0].type1, 1);
					indScan = ((BTreeFile) indFile).new_scan(null, key);
				} else {
					key = getValue(selects[0], selects[0].type2, 2);
					indScan = ((BTreeFile) indFile).new_scan(null, key);
				}
				return indScan;
			}

			// symbol > value
			if (selects[0].op.attrOperator == AttrOperator.aopGT) {
				if (selects[0].type1.attrType != AttrType.attrSymbol) {
					selects[0].operand1.integer++;
					key = getValue(selects[0], selects[0].type1, 1);
					indScan = ((BTreeFile) indFile).new_scan(key, null);
				} else {
					selects[0].operand2.integer++;
					key = getValue(selects[0], selects[0].type2, 2);
					indScan = ((BTreeFile) indFile).new_scan(key, null);
				}
				return indScan;
			}
			// or symbol >= value
			if (selects[0].op.attrOperator == AttrOperator.aopGE) {
				if (selects[0].type1.attrType != AttrType.attrSymbol) {
					key = getValue(selects[0], selects[0].type1, 1);
					indScan = ((BTreeFile) indFile).new_scan(key, null);
				} else {
					key = getValue(selects[0], selects[0].type2, 2);
					indScan = ((BTreeFile) indFile).new_scan(key, null);
				}
				return indScan;
			}

			// error if reached here
			System.err.println("Error -- in IndexUtils.BTree_scan()");
			return null;
		} else {
			// selects[1] != null, must be a range query
			if (selects[0].type1.attrType != AttrType.attrSymbol && selects[0].type2.attrType != AttrType.attrSymbol) {
				throw new InvalidSelectionException("IndexUtils.java: Invalid selection condition");
			}
			if (selects[1].type1.attrType != AttrType.attrSymbol && selects[1].type2.attrType != AttrType.attrSymbol) {
				throw new InvalidSelectionException("IndexUtils.java: Invalid selection condition");
			}

			// which symbol is higher??
			KeyClass key1, key2;
			AttrType type;

			if (selects[0].type1.attrType != AttrType.attrSymbol) {
				if (selects[0].op.attrOperator == AttrOperator.aopGT) {
					selects[0].operand1.integer++;
				}
				if (selects[0].op.attrOperator == AttrOperator.aopLT) {
					selects[0].operand1.integer--;
				}
				key1 = getValue(selects[0], selects[0].type1, 1);
				type = selects[0].type1;
			} else {
				if (selects[0].op.attrOperator == AttrOperator.aopGT) {
					selects[0].operand2.integer++;
				}
				if (selects[0].op.attrOperator == AttrOperator.aopLT) {
					selects[0].operand2.integer--;
				}
				key1 = getValue(selects[0], selects[0].type2, 2);
				type = selects[0].type2;
			}
			if (selects[1].type1.attrType != AttrType.attrSymbol) {
				if (selects[1].op.attrOperator == AttrOperator.aopGT) {
					selects[1].operand1.integer++;
				}
				if (selects[1].op.attrOperator == AttrOperator.aopLT) {
					selects[1].operand1.integer--;
				}
				key2 = getValue(selects[1], selects[1].type1, 1);
			} else {
				if (selects[1].op.attrOperator == AttrOperator.aopGT) {
					selects[1].operand2.integer++;
				}
				if (selects[1].op.attrOperator == AttrOperator.aopLT) {
					selects[1].operand2.integer--;
				}
				key2 = getValue(selects[1], selects[1].type2, 2);
			}

			switch (type.attrType) {
				case AttrType.attrString:
					if (((StringKey) key1).getKey().compareTo(((StringKey) key2).getKey()) < 0) {
						indScan = ((BTreeFile) indFile).new_scan(key1, key2);
					} else {
						indScan = ((BTreeFile) indFile).new_scan(key2, key1);
					}
					return indScan;

				case AttrType.attrInteger:
					if (((IntegerKey) key1).getKey().intValue() < ((IntegerKey) key2).getKey().intValue()) {
						indScan = ((BTreeFile) indFile).new_scan(key1, key2);
					} else {
						indScan = ((BTreeFile) indFile).new_scan(key2, key1);
					}
					return indScan;

				case AttrType.attrReal:
					/*
					 * if ((FloatKey)key1.getKey().floatValue() <
					 * (FloatKey)key2.getKey().floatValue()) {
					 * indScan = ((BTreeFile)indFile).new_scan(key1, key2);
					 * }
					 * else {
					 * indScan = ((BTreeFile)indFile).new_scan(key2, key1);
					 * }
					 * return indScan;
					 */
				default:
					// error condition
					throw new UnknownKeyTypeException(
							"IndexUtils.java: Only Integer and String keys are supported so far");
			}
		} // end of else

	}

	/**
	 * getValue returns the key value extracted from the selection condition.
	 * 
	 * @param cd     the selection condition
	 * @param type   attribute type of the selection field
	 * @param choice first (1) or second (2) operand is the value
	 * @return an instance of the KeyClass (IntegerKey or StringKey)
	 * @exception UnknownKeyTypeException only int and string keys are supported
	 */
	private static KeyClass getValue(CondExpr cd, AttrType type, int choice)
			throws UnknownKeyTypeException {
		// error checking
		if (cd == null) {
			return null;
		}
		if (choice < 1 || choice > 2) {
			return null;
		}

		switch (type.attrType) {
			case AttrType.attrString:
				if (choice == 1)
					return new StringKey(cd.operand1.string);
				else
					return new StringKey(cd.operand2.string);
			case AttrType.attrInteger:
				if (choice == 1)
					return new IntegerKey(Integer.valueOf(cd.operand1.integer));
				else
					return new IntegerKey(Integer.valueOf(cd.operand2.integer));
			case AttrType.attrReal:
				/*
				 * // need FloatKey class in bt.java
				 * if (choice == 1) return new FloatKey(new Float(cd.operand.real));
				 * else return new FloatKey(new Float(cd.operand.real));
				 */
			default:
				throw new UnknownKeyTypeException("IndexUtils.java: Only Integer and String keys are supported so far");
		}

	}

	public static IndexFileScan Bitmap_scan(Columnarfile cf,
			int columnNo,
			CondExpr[] selects,
			boolean indexOnly) throws IndexException {

		try {
			columnarfile = cf;
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			List<BitmapFileScan> scans = new ArrayList<>();
			for (String bmName : columnarfile.getAvailableBM(columnNo)) {
				if (evalBMName(bmName, columnNo)) {
					scans.add((new BitMapFile(bmName)).new_scan());
				}
			}
		} catch (Exception e) {
			// any exception is swalled into a Index Exception
			throw new IndexException(e, "IndexScan.java: BitMapFile exceptions caught from BitMapFile constructor");
		}
		return null;
	}

	static boolean evalBMName(String s, int _columnNo) throws Exception {
		if (_selects == null)
			return true;

		short[] _sizes = new short[1];
		_sizes[0] = columnarfile.getAttrsizeforcolumn(_columnNo);
		AttrType[] _types = new AttrType[1];
		_types[0] = columnarfile.getAttrtypeforcolumn(_columnNo);

		byte[] data = new byte[6 + _sizes[0]];
		String val = s.split("\\.")[3];
		if (_types[0].attrType == AttrType.attrInteger) {
			int t = Integer.parseInt(val);
			Convert.setIntValue(t, 6, data);
		} else {
			Convert.setStrValue(val, 6, data);
		}

		Tuple jTuple = new Tuple(data, 0, data.length);
		_sizes[0] -= 2;

		jTuple.setHdr((short) 1, _types, _sizes);

		if (PredEval.Eval(_selects, jTuple, null, _types, null)) {
			return true;
		}

		return false;
	}

}
