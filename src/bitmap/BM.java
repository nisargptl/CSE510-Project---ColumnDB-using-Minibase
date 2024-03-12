package bitmap;

import btree.PinPageException;
import btree.UnpinPageException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BM {

  public BM() {
    // Constructor
  }

  /**
   * For debug. Print the bitmap structure out.
   * @param header the head page of the bitmap file.
   * Assuming BitMapHeaderPage contains methods to access the bitmap data.
   * This example might need to be adjusted based on the actual implementation of BitMapHeaderPage.
   */
  public void printBitMap(BitMapHeaderPage header) {
    // Assuming BitMapHeaderPage provides a way to access the bitmap's size and individual bits.
    if (header == null) {
      System.out.println("Header information is missing.");
      return;
    }

    LinkedList<PageId> pinnedPages = new LinkedList<>();
    PageId bmPageId = header.get_rootId();

    if (bmPageId.pid == INVALID_PAGE) {
      System.out.println("Bitmap index file is empty.");
    } else {
      System.out.println("Column File Name: " + header.getColumnarFileName());
      System.out.println("Column Index: " + header.getColumnNumber());
      System.out.println("Data Type: " + header.getAttrType());
      System.out.println("Key Value: " + header.getValue());

      try {
        int bitPosition = 0;
        do {
          Page currentPage = pinSpecificPage(bmPageId);
          pinnedPages.add(bmPageId);

          BMPage bitmapPage = new BMPage(currentPage);
          BitSet bits = BitSet.valueOf(bitmapPage.getBMpageArray());
          int bitsCount = bitmapPage.getCounter();

          for (int bit = 0; bit < bitsCount; bit++) {
            System.out.println(
              "Bit Position: " +
              bitPosition +
              " - Value: " +
              (bits.get(bit) ? 1 : 0)
            );
            bitPosition++;
          }

          bmPageId = bitmapPage.getNextPage();
        } while (currentPageId.pid != INVALID_PAGE);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        pinnedPages.forEach(pageId -> {
          try {
            releasePage(pageId);
          } catch (BTreeException e) {
            e.printStackTrace();
          }
        });
      }
    }
  }

  /**
   * Generates bitmap indexes for each unique value in each column of a Columnarfile.
   *
   * @param cfName The name of the Columnarfile for which to generate bitmap indexes.
   * @throws Exception If there is an issue accessing the Columnarfile or generating the indexes.
   */
  public static void generateBitmapIndexes(String cfName) throws Exception {
    Columnarfile cf = new Columnarfile(cfName);
    // Assume getColumnTypes is a method that returns the types of columns in the Columnarfile
    AttrType[] types = cf.getColumnTypes();

    for (int columnNo = 0; columnNo < types.length; columnNo++) {
      // Assume getUniqueValues is a method that extracts all unique values from a specified column
      Set<Object> uniqueValues = cf.getUniqueValues(columnNo);

      for (Object value : uniqueValues) {
        // For each unique value, generate a bitmap index
        // Assume createBitMapIndex is a method to create a new bitmap index for a given value in a column
        cf.createBitMapIndex(columnNo, value);
      }
    }
  }

  /**
   * Uses bitmap indexes to find rows in a Columnarfile that match a given value in a specified column.
   *
   * @param cfName The name of the Columnarfile to query.
   * @param columnNo The column number to query.
   * @param value The value to match in the query.
   * @return A list of TIDs (tuple IDs) for tuples in the Columnarfile that match the given value.
   * @throws Exception If there is an issue accessing the Columnarfile or performing the query.
   */
  public static List<TID> queryColumnWithBitmap(
    String cfName,
    int columnNo,
    Object value
  ) throws Exception {
    Columnarfile cf = new Columnarfile(cfName);
    // Assume getBitmapForValue is a method that retrieves the bitmap index for a given value in a specified column
    BitSet bitmap = cf.getBitmapForValue(columnNo, value);
    List<TID> matchingTIDs = new ArrayList<>();

    for (int i = bitmap.nextSetBit(0); i >= 0; i = bitmap.nextSetBit(i + 1)) {
      // Assuming a way to convert bit index to TID
      TID tid = convertBitIndexToTID(i);
      matchingTIDs.add(tid);
    }

    return matchingTIDs;
  }
}
