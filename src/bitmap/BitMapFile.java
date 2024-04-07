package bitmap;

import btree.*;
import columnar.Columnarfile;
import columnar.ValueClass;
import diskmgr.*;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BitMapFile extends IndexFile implements GlobalConst {

  private BitMapHeaderPage headerPage;
  private PageId headerPageId;
  private final String fileName;
  private boolean isCompressed;


  public BitMapFile(String filename, boolean isCompressed) throws Exception {
    this.fileName = filename;
    this.isCompressed = isCompressed;

    headerPageId = get_file_entry(filename);
    if (headerPageId == null) {
      throw new GetFileEntryException(null, "file not found");
    }
    headerPage = new BitMapHeaderPage(headerPageId);
  }

  public BitMapFile(String filename, Columnarfile columnarFile, int columnNo, AttrType attrType, boolean isCompressed)
          throws Exception {
    this.fileName = filename;
    this.isCompressed = isCompressed;

    headerPageId = get_file_entry(filename);
    if (headerPageId == null) {
      headerPage = new BitMapHeaderPage();
      headerPageId = headerPage.getPageId();
      add_file_entry(filename, headerPageId);
      headerPage.set_rootId(new PageId(INVALID_PAGE));
      headerPage.setColumnarFileName(columnarFile.getColumnarFileName());
      headerPage.setColumnNumber(columnNo);
      // Use the attribute type's toString method or equivalent representation
      headerPage.setValue(attrType.toString());
      headerPage.setAttrType(attrType); // Now directly setting the AttrType
    } else {
      headerPage = new BitMapHeaderPage(headerPageId);
    }
  }


  public void close() throws Exception {
    if (headerPage != null) {
      SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
      headerPage = null;
    }
  }

  public void destroyBitMapFile() throws Exception {
    if (headerPage != null) {
      PageId currentPageId = headerPage.get_rootId();
      while (currentPageId.pid != INVALID_PAGE) {
        BMPage currentPage = new BMPage(pinPage(currentPageId));
        PageId nextPageId = currentPage.getNextPage();
        unpinPage(currentPageId, false);
        freePage(currentPageId);
        currentPageId = nextPageId;
      }
      unpinPage(headerPageId, false);
      freePage(headerPageId);
      delete_file_entry(fileName);
      headerPage = null;
    }
  }

  public BitMapHeaderPage getHeaderPage() {
    return headerPage;
  }

  public Boolean insert(int position) throws Exception {
    setValueAtPosition(true, position);
    return Boolean.TRUE;
  }

  public Boolean delete(int position) throws Exception {
    setValueAtPosition(false, position);
    return Boolean.TRUE;
  }

  private PageId get_file_entry(String filename) throws GetFileEntryException {
    try {
      return SystemDefs.JavabaseDB.get_file_entry(filename);
    } catch (Exception e) {
      throw new GetFileEntryException(e, "");
    }
  }

  private void add_file_entry(String filename, PageId pageno) throws AddFileEntryException {
    try {
      SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
    } catch (Exception e) {
      throw new AddFileEntryException(e, "");
    }
  }

  private void delete_file_entry(String filename) throws DeleteFileEntryException {
    try {
      SystemDefs.JavabaseDB.delete_file_entry(filename);
    } catch (Exception e) {
      throw new DeleteFileEntryException(e, "");
    }
  }

  private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    } catch (Exception e) {
      throw new UnpinPageException(e, "");
    }
  }

  private void freePage(PageId pageno) throws FreePageException {
    try {
      SystemDefs.JavabaseBM.freePage(pageno);
    } catch (Exception e) {
      throw new FreePageException(e, "");
    }
  }

  private Page pinPage(PageId pageno) throws PinPageException {
    try {
      Page page = new Page();
      SystemDefs.JavabaseBM.pinPage(pageno, page, false);
      return page;
    } catch (Exception e) {
      throw new PinPageException(e, "");
    }
  }

  private void setValueAtPosition(boolean set, int position) throws Exception {
    List<PageId> pinnedPages = new ArrayList<>();
    if (headerPage == null) {
      throw new Exception("Bitmap header page is null");
    }

    if (headerPage.get_rootId().pid == INVALID_PAGE) {
      // If no root page exists, create one
      PageId newPageId = getNewBMPage(headerPageId);
      pinnedPages.add(newPageId);
      headerPage.set_rootId(newPageId);
    }

    PageId currentPageId = headerPage.get_rootId();
    Page currentPage = pinPage(currentPageId);
    pinnedPages.add(currentPageId);
    BMPage bmPage = new BMPage(currentPage);

    byte[] bitmapData;
    if (this.isCompressed) {
      // If compressed, decode the RLE to manipulate the bitmap
      bitmapData = CBitMapPage.decodeRLE(bmPage.getBMpageArray());
    } else {
      // If uncompressed, work directly with the bitmap array
      bitmapData = bmPage.getBMpageArray();
    }

    // Ensure bitmapData is large enough to include the position
    int requiredSize = (position / 8) + 1;
    if (bitmapData.length < requiredSize) {
      byte[] newBitmapData = new byte[requiredSize];
      System.arraycopy(bitmapData, 0, newBitmapData, 0, bitmapData.length);
      bitmapData = newBitmapData;
    }

    // Set or clear the bit at the specified position
    int bytePos = position / 8;
    int bitPos = position % 8;
    if (set) {
      bitmapData[bytePos] |= (1 << bitPos);
    } else {
      bitmapData[bytePos] &= ~(1 << bitPos);
    }

    // If compressed, re-encode the bitmap using RLE
    if (this.isCompressed) {
      byte[] encodedBitmapData = CBitMapPage.encodeRLE(bitmapData);
      bmPage.writeBMPageArray(encodedBitmapData);
    } else {
      // If uncompressed, write the modified bitmap directly
      bmPage.writeBMPageArray(bitmapData);
    }

    // Update the page counter if necessary
    if (bmPage.getCounter() < position + 1) {
      bmPage.updateCounter((short) (position + 1));
    }

    // Unpin all used pages
    for (PageId pinnedPage : pinnedPages) {
      unpinPage(pinnedPage, true);
    }
  }


  private PageId getNewBMPage(PageId prevPageId) throws Exception {
    Page apage = new Page();
    PageId pageId = newPage(apage, 1);
    BMPage bmPage = new BMPage();
    bmPage.init(pageId, apage);
    bmPage.setPrevPage(prevPageId);

    return pageId;
  }

  private PageId newPage(Page page, int num) throws HFBufMgrException {
    PageId tmpId = new PageId();
    try {
      tmpId = SystemDefs.JavabaseBM.newPage(page, num);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
    }
    return tmpId;
  }

  public void scanClose() throws Exception {
    if (headerPage != null) {
      SystemDefs.JavabaseBM.unpinPage(headerPageId, false);
      headerPage = null;
    }
  }

  public BitmapFileScan new_scan() throws Exception {
    return new BitmapFileScan(this);
  }

  @Override
  public void insert(KeyClass data, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException {
    System.out.println("Insert function");
  }

  @Override
  public boolean Delete(KeyClass data, RID rid) throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException, RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException, ConstructPageException, DeleteRecException, IndexSearchException, IOException {
    return false;
  }
}
