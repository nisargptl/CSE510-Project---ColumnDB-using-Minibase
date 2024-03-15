package bitmap;

import diskmgr.*;
import global.*;
import heap.*;
import java.io.IOException;

public class BMPage extends HFPage implements GlobalConst {

  public static final int DPFIXED = 2 * 2 + 3 * 4;
  public static final int NUM_POSITIONS_IN_A_PAGE = (MAX_SPACE - DPFIXED)*8;

  public static final int COUNTER = 0;
  public static final int FREE_SPACE = 2;
  public static final int PREV_PAGE = 4;
  public static final int NEXT_PAGE = 8;
  public static final int CUR_PAGE = 12;


  private PageId curPage = new PageId();
  private short counter;
  private short freeSpace;
  private PageId prevPage = new PageId();
  private PageId nextPage = new PageId();

  public BMPage() {
    super(); // Call HFPage constructor
  }

  public BMPage(Page page) {
    super(page); // Initialize this BMPage with the given Page
  }

  @Override
  public int available_space() {
    // Assuming each bit is a bitmap entry, calculate the number of available bits
    // This is a simplified example. Actual implementation might need to account for metadata, etc.
    try {
      int freeSpace = Convert.getShortValue(FREE_SPACE, getpage()); // Free space in bytes
      return freeSpace * 8; // Convert bytes to bits for bitmap
    } catch (IOException e) {
      return 0;
    }
  }

  @Override
  public void dumpPage() {
    // Dump contents of a bitmap page (example method, actual output would depend on bitmap structure)
    try {
      System.out.println("BMPage Dump:");
      System.out.println("Page ID: " + getCurPage().pid);
      System.out.println("Next Page ID: " + getNextPage().pid);
      System.out.println("Previous Page ID: " + getPrevPage().pid);
      // This would ideally print the bitmap's actual content. Placeholder for now.
      System.out.println("Bitmap Content: [Placeholder]");
    } catch (IOException e) {
      System.err.println("Error dumping BMPage: " + e.getMessage());
    }
  }

  @Override
  public boolean empty() {
    // Determine if the bitmap page is empty (simplified)
    try {
      return available_space() == MAX_SPACE * 8; // Assuming entire page is available
    } catch (Exception e) {
      return true;
    }
  }

  public void init(PageId pageNo, Page apage) throws IOException {
    data = apage.getpage();

    counter = (short) 0;
    Convert.setShortValue(counter, COUNTER, data);

    curPage.pid = pageNo.pid;
    Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    nextPage.pid = prevPage.pid = INVALID_PAGE;
    Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

    freeSpace = (short) NUM_POSITIONS_IN_A_PAGE;    // amount of space available
    Convert.setShortValue(freeSpace, FREE_SPACE, data);

    for (int i = DPFIXED; i < MAX_SPACE; i++) {
      Convert.setByteValue((byte) 0, i, data);
    }
  }

  public void openBMpage(Page apage) {
    data = apage.getpage();
  }

  public PageId getCurPage()
          throws IOException {
    curPage.pid = Convert.getIntValue(CUR_PAGE, data);
    return curPage;
  }

  public void setCurPage(PageId pageNo)
          throws IOException {
    curPage.pid = pageNo.pid;
    Convert.setIntValue(curPage.pid, CUR_PAGE, data);
  }

  public PageId getNextPage()
          throws IOException {
    nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
    return nextPage;
  }

  public void setNextPage(PageId pageNo)
          throws IOException {
    nextPage.pid = pageNo.pid;
    Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
  }

  public PageId getPrevPage()
          throws IOException {
    prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
    return prevPage;
  }

  public void setPrevPage(PageId pageNo)
          throws IOException {
    prevPage.pid = pageNo.pid;
    Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
  }

  public int getBit(int position) {
    // This method returns the value of the bit at the specified position
    int byteOffset = position / 8;
    int bitOffset = position % 8;
    byte[] pageData = getpage();

    if (byteOffset < 0 || byteOffset >= pageData.length) {
      return -1; // Indicates an error or invalid position
    }

    byte targetByte = pageData[byteOffset];
    return (targetByte >> (7 - bitOffset)) & 1; // Extract the specific bit and return its value
  }


  public byte[] getBMpageArray() throws Exception {
    int numBytesInPage = NUM_POSITIONS_IN_A_PAGE /8;
    byte[] bitMapArray = new byte[numBytesInPage];
    for (int i = 0; i < numBytesInPage; i++) {
      bitMapArray[i] = Convert.getByteValue(DPFIXED + i, data);
    }
    return bitMapArray;
  }

  void writeBMPageArray(byte[] givenData) throws Exception {
    int count = givenData.length;
    for (int i = 0; i < count; i++) {
      Convert.setByteValue(givenData[i], DPFIXED + i, data);
    }
  }

  public Integer getCounter() throws Exception {
    return (int) Convert.getShortValue(COUNTER, data);
  }

  public void updateCounter(Short value) throws Exception {
    Convert.setShortValue(value, COUNTER, data);
    Convert.setShortValue((short) (NUM_POSITIONS_IN_A_PAGE - value), FREE_SPACE, data);
  }

}
