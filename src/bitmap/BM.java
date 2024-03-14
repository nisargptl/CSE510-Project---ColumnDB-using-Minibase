package bitmap;

import btree.PinPageException;
import btree.UnpinPageException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import global.TID;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

public class BM implements GlobalConst {

  public BM() {
    // Constructor
  }

  /**
   * For debug. Print the bitmap structure out.
   * @param header the head page of the bitmap file.
   * Assuming BitMapHeaderPage contains methods to access the bitmap data.
   * This example might need to be adjusted based on the actual implementation of BitMapHeaderPage.
   */
  public static void printBitMap(BitMapHeaderPage header) throws Exception {
    if (header == null) {
      System.out.println("\n Empty Header!!!");
    } else {
      List<PageId> pinnedPages = new ArrayList<>();
      PageId bmPageId = header.get_rootId();
      if (bmPageId.pid == INVALID_PAGE) {
        System.out.println("Empty Bitmap File");
        return;
      }
      System.out.println("Columnar File Name: " + header.getColumnarFileName());
      System.out.println("Column Number: " + header.getColumnNumber());
      System.out.println("Attribute Type: " + header.getAttrType());
      System.out.println("Attribute Value: " + header.getValue());
      Page page = pinPage(bmPageId);
      pinnedPages.add(bmPageId);
      BMPage bmPage = new BMPage(page);
      int position = 0;
      while (Boolean.TRUE) {
        int count = bmPage.getCounter();
        BitSet currentBitSet = BitSet.valueOf(bmPage.getBMpageArray());
        for (int i = 0; i < count; i++) {
          System.out.println("Position: " + position + "   Value: " + (currentBitSet.get(i) ? 1 : 0));
          position++;
        }
        if (bmPage.getNextPage().pid == INVALID_PAGE) {
          break;
        } else {
          page = pinPage(bmPage.getNextPage());
          pinnedPages.add(bmPage.getNextPage());
          bmPage.openBMpage(page);
        }
      }
      for (PageId pageId : pinnedPages) {
        unpinPage(pageId);
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

  /***
   * Unpin the given page
   * @param pageno
   * @throws UnpinPageException
   */
  private static void unpinPage(PageId pageno)
          throws UnpinPageException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
    } catch (Exception e) {
      e.printStackTrace();
      throw new UnpinPageException(e, "");
    }
  }

  /***
   * Pin the page passed as input
   * @param pageno
   * @return
   * @throws PinPageException
   */
  private static Page pinPage(PageId pageno) throws PinPageException {
    try {
      Page page = new Page();
      SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
      return page;
    } catch (Exception e) {
      e.printStackTrace();
      throw new PinPageException(e, "");
    }
  }
}
