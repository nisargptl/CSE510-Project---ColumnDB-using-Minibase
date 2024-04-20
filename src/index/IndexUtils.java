package index;

import bitmap.BitMapFile;
import bitmap.BitmapFileScan;
import columnar.Columnarfile;
import global.*;
import btree.*;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import iterator.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
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

			// symbol < value or symbol <= value
			if (selects[0].op.attrOperator == AttrOperator.aopLT || selects[0].op.attrOperator == AttrOperator.aopLE) {
				if (selects[0].type1.attrType != AttrType.attrSymbol) {
					key = getValue(selects[0], selects[0].type1, 1);
					indScan = ((BTreeFile) indFile).new_scan(null, key);
				} else {
					key = getValue(selects[0], selects[0].type2, 2);
					indScan = ((BTreeFile) indFile).new_scan(null, key);
				}
				return indScan;
			}

			// symbol > value or symbol >= value
			if (selects[0].op.attrOperator == AttrOperator.aopGT || selects[0].op.attrOperator == AttrOperator.aopGE) {
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
				key1 = getValue(selects[0], selects[0].type1, 1);
				type = selects[0].type1;
			} else {
				key1 = getValue(selects[0], selects[0].type2, 2);
				type = selects[0].type2;
			}
			if (selects[1].type1.attrType != AttrType.attrSymbol) {
				key2 = getValue(selects[1], selects[1].type1, 1);
			} else {
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

	// public static IndexFileScan Bitmap_scan(Columnarfile columnarFile, int
	// columnNo, CondExpr[] selects,
	// boolean indexOnly) throws IndexException {
	// try {
	// List<BitmapFileScan> scans = new ArrayList<>();
	// for (String bmName : columnarFile.getAvailableBM(columnNo)) {
	// scans.add((new BitMapFile(bmName)).new_scan());
	// }

	// return new IndexFileScan() {
	// private BitSet bitMaps = new BitSet();
	// private int scanCounter = 0;
	// private int counter = 0;

	// @Override
	// public KeyDataEntry get_next() {
	// int position = get_next_position(scans);
	// if (position < 0)
	// return null;

	// IntegerKey key = new IntegerKey(position);
	// RID rid = new RID(); // Dummy RID, replace with actual if needed
	// return new KeyDataEntry(key, rid);
	// }

	// @Override
	// public void close() throws Exception {
	// for (BitmapFileScan s : scans) {
	// s.close();
	// }
	// }

	// @Override
	// public void delete_current()
	// throws ScanDeleteException, FieldNumberOutOfBoundException, IOException,
	// IndexException {
	// throw new UnsupportedOperationException("Delete operation not supported on
	// bitmap scans.");
	// }

	// @Override
	// public int keysize() {
	// return Integer.SIZE / Byte.SIZE; // This returns 4, as an integer is
	// typically 4 bytes.
	// }

	// private int get_next_position(List<BitmapFileScan> scans) {
	// try {
	// if (scanCounter == 0 || scanCounter > counter) {
	// bitMaps.clear();
	// for (BitmapFileScan s : scans) {
	// counter = s.counter;
	// BitSet bs = s.get_next_bitmap();
	// if (bs == null) {
	// return -1;
	// } else {
	// bitMaps.or(bs);
	// }
	// }
	// }
	// while (scanCounter <= counter) {
	// if (bitMaps.get(scanCounter)) {
	// int position = scanCounter++;
	// return position;
	// } else {
	// scanCounter++;
	// }
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// return -1;
	// }
	// };
	// } catch (Exception e) {
	// throw new IndexException(e, "Bitmap_scan: exceptions caught");
	// }
	// }

	public static IndexFileScan Bitmap_scan(Columnarfile columnarFile, int columnNo, CondExpr[] selects,
											boolean indexOnly) throws IndexException {
		try {
			List<BitmapFileScan> scans = new ArrayList<>();

			// Loop through available bitmap files for the specified column
			System.out.println("COLUMN NUMBER: " + columnNo);
			for (String bmName : columnarFile.getAvailableBM(columnNo)) {
				System.out.println("BM NAME INSIDE LOOP: " + bmName);
				BitMapFile bitmapFile = new BitMapFile(bmName);  // Open each bitmap file
				int totalPositions = bitmapFile.getTotalPositions();  // Get total positions in the bitmap

				// Check up to the total number of positions in the bitmap or 2000, whichever is smaller
				for (int position = 0; position < Math.min(totalPositions, 2000); position++) {
					if (evaluateCondition(selects, bitmapFile, position)) {
						scans.add(bitmapFile.new_scan());  // Add a new scan if condition is met
						break; // Once a match is found, no need to check further positions in the same bitmap
					}
				}
			}

			return generateIndexFileScan(scans);  // Generate and return a composite index file scan
		} catch (Exception e) {
			throw new IndexException(e, "Bitmap_scan: exceptions caught");
		}
	}

	private static boolean evaluateCondition(CondExpr[] selects, BitMapFile bitmapFile, int position) {
		if (selects == null || selects[0] == null)
			return true; // No condition, include all bitmaps.

		boolean matches = true;
		try {
			for (CondExpr cond : selects) {
				if (cond != null) {
					// Assuming the value at the position in bitmap file can be evaluated here:
					Object bitmapValue = bitmapFile.getValueAtPosition(position);  // Get the value at the bitmap position
					Object condValue = getValueFromCondExpr(cond);
					if (!applyOperator(cond.op, bitmapValue, condValue)) {
						matches = false;
						break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			matches = false;
		}
		return matches;
	}


//	private static boolean evaluateCondition(CondExpr[] selects, BitMapFile bitmapFile, int position) {
//		if (selects == null || selects[0] == null) {
//			return true; // No condition, include all bitmaps.
//		}
//
//		try {
//			boolean matches = true;
//			for (CondExpr cond : selects) {
//				System.out.println(Arrays.toString(selects));
//				if (cond != null) {
//					Object bitmapValue = bitmapFile.getValueAtPosition(position); // Assuming this fetches the correct value
//					System.out.println(bitmapValue);
//					Object condValue = getValueFromCondExpr(cond);
//
//					// Log for debugging purposes
//					System.out.println("Evaluating: " + bitmapValue + " " + cond.op.attrOperator + " " + condValue);
//
//					if (!applyOperator(cond.op, bitmapValue, condValue)) {
//						matches = false;
//						break;
//					}
//				}
//			}
//			return matches;
//		} catch (Exception e) {
//			e.printStackTrace();
//			return false;
//		}
//	}


	private static boolean applyOperator(AttrOperator op, Object bitmapValue, Object condValue) {
		if (bitmapValue == null || condValue == null) {
			return false;
		}

		// Cast and compare based on data type
		switch (op.attrOperator) {
			case AttrOperator.aopEQ:
				return bitmapValue.equals(condValue);
			case AttrOperator.aopLT:
				if (bitmapValue instanceof Comparable && condValue instanceof Comparable) {
					return ((Comparable) bitmapValue).compareTo(condValue) < 0;
				}
				break;
			case AttrOperator.aopGT:
				if (bitmapValue instanceof Comparable && condValue instanceof Comparable) {
					return ((Comparable) bitmapValue).compareTo(condValue) > 0;
				}
				break;
			// Add more cases as necessary
			default:
				return false;
		}
		return false;
	}

	// new added here
	// ***********************************************************
	private static Object extractValueFromBitmapName(String bmName) {
		// Logic to translate the bitmap file name into the data value it represents.
		// Example, if bmName is "column_value", return the value part.
		return parseValueFromName(bmName);
	}

	private static Object parseValueFromName(String bmName) {
		// Assuming the bitmap's name encodes the value directly after an underscore
		int underscoreIndex = bmName.indexOf('_'); // Find the position of the underscore
		if (underscoreIndex != -1 && underscoreIndex + 1 < bmName.length()) {
			return bmName.substring(underscoreIndex + 1); // Extracts the part after the underscore
		}
		return null; // Return null if name is not as expected
	}

//	private static boolean evaluateCondition(CondExpr[] selects, Object bitmapValue) {
//		if (selects == null || selects[0] == null)
//			return true; // No condition, include all bitmaps.
//
//		boolean matches = true;
//		for (CondExpr cond : selects) {
//			if (cond != null) {
//				Object condValue = getValueFromCondExpr(cond);
//				System.out.println("condValue: " + condValue);
//				// System.out.println("cond.op.attrOperator, bitmapValue, condValue");
//				// if (!applyOperator(cond.op, bitmapValue, condValue)) {
//				// matches = false;
//				// break;
//				// }
//			}
//		}
//		return matches;
//	}

	private static Object getValueFromCondExpr(CondExpr expr) {
		// Assuming the actual value is always in operand2 (you'll need to adjust this
		// logic based on actual cases)
		return expr.type2.attrType == AttrType.attrInteger ? expr.operand2.integer : expr.operand2.string;
	}

	// private static boolean applyOperator(AttrOperator op, Object bitmapValue,
	// Object condValue) {
	// // This is a simplified example. You should expand it based on data types and
	// // available operators.
	// switch (op.attrOperator) {
	// case AttrOperator.aopEQ:
	// return bitmapValue.equals(condValue);
	// case AttrOperator.aopLT:
	// return ((Comparable) bitmapValue).compareTo(condValue) < 0;
	// case AttrOperator.aopGT:
	// return ((Comparable) bitmapValue).compareTo(condValue) > 0;
	// default:
	// return false;
	// }
	// }
	// new added till here
	// ***********************************************************

	private static IndexFileScan generateIndexFileScan(List<BitmapFileScan> scans) {
		return new IndexFileScan() {
			private BitSet bitMaps = new BitSet();
			private int scanCounter = 0;
			private int counter = 0;

			@Override
			public KeyDataEntry get_next() {
				int position = get_next_position(scans);
				if (position < 0)
					return null;

				IntegerKey key = new IntegerKey(position);
				RID rid = new RID(); // Always a dummy RID in this case
				return new KeyDataEntry(key, rid);
			}

			@Override
			public void close() throws Exception {
				for (BitmapFileScan s : scans) {
					s.close();
				}
			}

			@Override
			public void delete_current()
					throws ScanDeleteException, FieldNumberOutOfBoundException, IOException,
					IndexException {
				throw new UnsupportedOperationException("Delete operation not supported on bitmap scans.");
			}

			@Override
			public int keysize() {
				return Integer.SIZE / Byte.SIZE; // This returns 4, as an integer is typically 4 bytes.
			}

			private int get_next_position(List<BitmapFileScan> scans) {
				try {
					if (scanCounter == 0 || scanCounter > counter) {
						bitMaps.clear();
						for (BitmapFileScan s : scans) {
							counter = s.counter;
							BitSet bs = s.get_next_bitmap();
							if (bs == null) {
								return -1;
							} else {
								bitMaps.or(bs);
							}
						}
					}
					while (scanCounter <= counter) {
						if (bitMaps.get(scanCounter)) {
							int position = scanCounter++;
							return position;
						} else {
							scanCounter++;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return -1;
			}
		};
	}
}
