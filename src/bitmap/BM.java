package bitmap;

import btree.PinPageException;
import btree.UnpinPageException;
import diskmgr.Page;
import global.*;

import java.io.ByteArrayOutputStream;
import java.util.*;

public class BM implements GlobalConst {

  public BM() {
    // BM Constructor
  }

  public static BitSet getBitMap(BitMapHeaderPage header) throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );

    if (header == null) {
      System.out.println("\n Empty Header!!!");
    } else {
      List<PageId> pinnedPages = new ArrayList<>();
      PageId bmPageId = header.get_rootId();
      if (bmPageId.pid == INVALID_PAGE) {
        System.out.println("Empty Bitmap File");
        return null;
      }
      Page page = pinPage(bmPageId);
      pinnedPages.add(bmPageId);
      BMPage bmPage = new BMPage(page);

      while (true) {
        outputStream.write(bmPage.getBMpageArray());
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
    return BitSet.valueOf(outputStream.toByteArray());
  }


  public static void printBitMap(BitMapHeaderPage header) throws Exception {
    if (header == null) {
      System.out.println("\n Empty Header!!!");
      return;
    }

    System.out.println("Bitmap File Information:");
    PageId bmPageId = header.get_rootId();
    if (bmPageId.pid == INVALID_PAGE) {
      System.out.println("No Bitmap Data");
      return;
    }

    // Printing columnar file information
    System.out.println("Columnar File: " + header.getColumnarFileName());
    System.out.println("Column #: " + header.getColumnNumber());
    System.out.println("Attribute Type: " + header.getAttrType());
    System.out.println("Value: " + header.getValue());

    // Process bitmap pages
    processBitMapPages(bmPageId);
  }

  // Process and print bitmap pages
  private static void processBitMapPages(PageId bmPageId) throws Exception {
    List<PageId> pinnedPages = new LinkedList<>();
    int position = 0;

    do {
      Page page = pinPage(bmPageId);
      pinnedPages.add(bmPageId);
      BMPage bmPage = new BMPage(page);

      BitSet bitSet = BitSet.valueOf(bmPage.getBMpageArray());
      for (int i = 0; i < bmPage.getCounter(); i++) {
        System.out.println("Pos: " + position++ + " Value: " + (bitSet.get(i) ? 1 : 0));
      }

      bmPageId = bmPage.getNextPage();
    } while (bmPageId.pid != INVALID_PAGE);

    // Unpinning pages after processing
    for (PageId pageId : pinnedPages) {
      unpinPage(pageId);
    }
  }

  // Unpin a page with exception handling
  private static void unpinPage(PageId pageno) throws UnpinPageException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, false);
    } catch (Exception e) {
      e.printStackTrace();
      throw new UnpinPageException(e, "UnpinPage Failed");
    }
  }

  // Pin a page with exception handling
  private static Page pinPage(PageId pageno) throws PinPageException {
    try {
      Page page = new Page();
      SystemDefs.JavabaseBM.pinPage(pageno, page, false);
      return page;
    } catch (Exception e) {
      e.printStackTrace();
      throw new PinPageException(e, "PinPage Failed");
    }
  }
}
