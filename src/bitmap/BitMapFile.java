package bitmap;

import btree.*;
import bufmgr.*;
import columnar.Columnarfile;
import diskmgr.*;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// public class BitMapFile extends IndexFile {

public class BitMapFile extends IndexFile implements GlobalConst {

  private BitMapHeaderPage headerPage;
  private PageId headerPageId;
  private String fileName;

  public BitMapHeaderPage getHeaderPage() {
    return headerPage;
  }

  public BitMapFile(String filename)
          throws Exception {
    headerPageId = get_file_entry(filename);
    if (headerPageId == null) {
      throw new GetFileEntryException(null, "file not found");
    }
    headerPage = new BitMapHeaderPage(headerPageId);
    String dbname = new String(filename);
  }

  public BitMapFile(
          String filename,
          Columnarfile columnarFile,
          int columnNo,
          ValueClass value
  )
          throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException, Exception {
    headerPageId = get_file_entry(filename);
    if (headerPageId == null) //file does not exist
    {
      headerPage = new BitMapHeaderPage();
      headerPageId = headerPage.getPageId();
      add_file_entry(filename, headerPageId);
      headerPage.set_rootId(new PageId(INVALID_PAGE));
      headerPage.setColumnarFileName(columnarFile.getColumnarFileName());
      headerPage.setColumnNumber(columnNo);
      if (value instanceof ValueInt) {
        headerPage.setValue(value.getValue().toString());
        headerPage.setAttrType(new AttrType(AttrType.attrInteger));
      } else {
        headerPage.setValue(value.getValue().toString());
        headerPage.setAttrType(new AttrType(AttrType.attrString));
      }
    } else {
      headerPage = new BitMapHeaderPage(headerPageId);
    }
  }

  @Override
  public void insert(KeyClass data, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException {

  }

  @Override
  public boolean Delete(KeyClass data, RID rid) throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException, RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException, ConstructPageException, DeleteRecException, IndexSearchException, IOException {
    return false;
  }

  public void close()
    throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
    if (headerPage != null) {
      SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
      headerPage = null;
    }
  }

  public void destroyBitMapFile() throws Exception {
    // Ensure complete traversal and deallocation of bitmap pages
    PageId currentPageId = headerPage.get_rootId();
    BMPage bmPage = new BMPage();
    while (currentPageId.pid != -1) {
      BMPage currentPage = new BMPage(pinPage(currentPageId));
      PageId nextPageId = currentPage.getNextPage();
      unpinPage(currentPageId); // Unpin the current page
      freePage(currentPageId); // Free the current page
      currentPageId = nextPageId; // Move to the next page
    }
    // Unpin and free the header page
    unpinPage(headerPageId);
    freePage(headerPageId);
    delete_file_entry(dbname);
    headerPage = null;

    if (headerPage != null) {
      PageId pgId = headerPage.get_rootId();
      BMPage bmPage = new BMPage();
      while (pgId.pid != INVALID_PAGE) {
        Page page = pinPage(pgId);
        bmPage.openBMpage(page);
        pgId = bmPage.getNextPage();
        unpinPage(pgId);
        freePage(pgId);
      }
      unpinPage(headerPageId);
      freePage(headerPageId);
      delete_file_entry(fileName);
      headerPage = null;
    }
  }

  public Boolean delete(int position) throws Exception {
    setValueAtPosition(false, position);
    return Boolean.TRUE;
  }

  public Boolean insert(int position) throws Exception {
    setValueAtPosition(true, position);
    return Boolean.TRUE;
  }

  // Utility methods similar to those in BTreeFile, adapted for bitmap files
  private PageId get_file_entry(String filename) throws GetFileEntryException {
    try {
      return SystemDefs.JavabaseDB.get_file_entry(filename);
    } catch (Exception e) {
      e.printStackTrace();
      throw new GetFileEntryException(e, "");
    }
  }

  private void add_file_entry(String filename, PageId pageno)
    throws AddFileEntryException {
    try {
      SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
    } catch (Exception e) {
      e.printStackTrace();
      throw new AddFileEntryException(e, "");
    }
  }

  private void delete_file_entry(String filename)
    throws DeleteFileEntryException {
    try {
      SystemDefs.JavabaseDB.delete_file_entry(filename);
    } catch (Exception e) {
      e.printStackTrace();
      throw new DeleteFileEntryException(e, "");
    }
  }

  private void unpinPage(PageId pageno) throws UnpinPageException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, false/* not DIRTY */);
    } catch (Exception e) {
      e.printStackTrace();
      throw new UnpinPageException(e, "");
    }
  }

  private void unpinPage(PageId pageno, boolean dirty)
          throws UnpinPageException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    } catch (Exception e) {
      e.printStackTrace();
      throw new UnpinPageException(e, "");
    }
  }


  private void freePage(PageId pageno) throws FreePageException {
    try {
      SystemDefs.JavabaseBM.freePage(pageno);
    } catch (Exception e) {
      e.printStackTrace();
      throw new FreePageException(e, "");
    }
  }

  private Page pinPage(PageId pageno)
          throws PinPageException {
    try {
      Page page = new Page();
      SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
      return page;
    } catch (Exception e) {
      e.printStackTrace();
      throw new PinPageException(e, "");
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
      for (int i = 1; i < pageCounter; i++) {
        bmPageId = bmPage.getNextPage();
        if (bmPageId.pid == BMPage.INVALID_PAGE) {
          PageId newPageId = getNewBMPage(bmPage.getCurPage());
          pinnedPages.add(newPageId);
          bmPage.setNextPage(newPageId);
          bmPageId = newPageId;
        }
        page = pinPage(bmPageId);
        bmPage = new BMPage(page);
      }
      byte[] currData = bmPage.getBMpageArray();
      int bytoPos = position/8;
      int bitPos = position%8;
      if(set)
        currData[bytoPos] |= (1<<bitPos);
      else
        currData[bytoPos] &= ~(1<<bitPos);
      bmPage.writeBMPageArray(currData);
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

  @Override
  public void insert(KeyClass data, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException {

  }

  @Override
  public boolean Delete(KeyClass data, RID rid) throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException, RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException, ConstructPageException, DeleteRecException, IndexSearchException, IOException {
    return false;
  }
}
