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

  public BitMapFile(String filename) throws Exception {
    this.fileName = filename;
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
    if (headerPage.get_rootId().pid != INVALID_PAGE) {
      int pageCounter = 1;
      while (position >= BMPage.NUM_POSITIONS_IN_A_PAGE) {
        pageCounter++;
        position -= BMPage.NUM_POSITIONS_IN_A_PAGE;
      }

      PageId bmPageId = headerPage.get_rootId();
      Page page = pinPage(bmPageId);
      pinnedPages.add(bmPageId);
      BMPage bmPage = new BMPage(page);

      byte[] currData;
      if (this.isCompressed) {
        currData = CBitMapPage.decodeRLE(bmPage.getBMpageArray());
      } else {
        currData = bmPage.getBMpageArray();
      }

      if (currData == null || currData.length == 0) {
        currData = new byte[(position / 8) + 1];  // Ensure there is enough space
      }

      int bytePos = position / 8;
      if (bytePos >= currData.length) {
        byte[] newCurrData = new byte[bytePos + 1];
        System.arraycopy(currData, 0, newCurrData, 0, currData.length);
        currData = newCurrData;
      }

      int bitPos = position % 8;
      if (set) {
        currData[bytePos] |= (1 << bitPos);
      } else {
        currData[bytePos] &= ~(1 << bitPos);
      }

      if (this.isCompressed) {
        byte[] newRleData = CBitMapPage.encodeRLE(currData);
        bmPage.writeBMPageArray(newRleData);
      } else {
        bmPage.writeBMPageArray(currData);
      }

      if (bmPage.getCounter() < position + 1) {
        bmPage.updateCounter((short) (position + 1));
      }
    } else {
      PageId newPageId = getNewBMPage(headerPageId);
      pinnedPages.add(newPageId);
      headerPage.set_rootId(newPageId);
      setValueAtPosition(set, position);
    }
    for (PageId pinnedPage : pinnedPages) {
      unpinPage(pinnedPage, true);
    }
  }

  public int getTotalPositions() throws Exception {
    if (headerPage == null) {
      throw new Exception("Bitmap header page is null");
    }
    return headerPage.getCounter();
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
  public void insert(KeyClass data, RID rid)
          throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
          ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
          DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException {
    System.out.println("Insert function");
  }

  @Override
  public boolean Delete(KeyClass data, RID rid) throws DeleteFashionException, LeafRedistributeException,
          RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException, IndexInsertRecException,
          FreePageException, RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException,
          IteratorException, ConstructPageException, DeleteRecException, IndexSearchException, IOException {
    return false;
  }
}
