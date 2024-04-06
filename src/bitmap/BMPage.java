package bitmap;

import diskmgr.*;
import global.*;
import heap.*;
import java.io.IOException;

public class BMPage extends HFPage implements GlobalConst {

  public static final int DPFIXED = 2 * 2 + 3 * 4;
  public static final int NUM_POSITIONS_IN_A_PAGE = (MAX_SPACE - DPFIXED) * 8;

  public static final int COUNTER = 0;
  public static final int FREE_SPACE = 2;
  public static final int PREV_PAGE = 4;
  public static final int NEXT_PAGE = 8;
  public static final int CUR_PAGE = 12;

  private final PageId curPage = new PageId();
  private short counter;
  private short freeSpace;
  private final PageId prevPage = new PageId();
  private final PageId nextPage = new PageId();

  public BMPage() {
  }

  public BMPage(Page page) {
    data = page.getpage();
  }

  @Override
  public int available_space() throws IOException {
    return Convert.getShortValue(FREE_SPACE, getpage()) * 8;
  }

  @Override
  public void dumpPage() {
    try {
      System.out.println("BMPage Dump:");
      System.out.println("Page ID: " + getCurPage().pid);
      System.out.println("Next Page ID: " + getNextPage().pid);
      System.out.println("Previous Page ID: " + getPrevPage().pid);
      System.out.println("Bitmap Content: [Placeholder for actual bitmap content]");
    } catch (IOException e) {
      System.err.println("Error dumping BMPage: " + e.getMessage());
    }
  }

  @Override
  public boolean empty() throws IOException {
    return available_space() == MAX_SPACE * 8;
  }

  public void init(PageId pageNo, Page apage) throws IOException {
    data = apage.getpage();

    setCurPage(pageNo);
    setPrevPage(new PageId(INVALID_PAGE));
    setNextPage(new PageId(INVALID_PAGE));
    freeSpace = (short) (MAX_SPACE - DPFIXED);
    Convert.setShortValue(freeSpace, FREE_SPACE, data);

    clearSpace(DPFIXED, MAX_SPACE - DPFIXED);
  }

  public void openBMpage(Page apage) {
    data = apage.getpage();
  }

  public PageId getCurPage() throws IOException {
    return new PageId(Convert.getIntValue(CUR_PAGE, data));
  }

  public void setCurPage(PageId pageNo) throws IOException {
    Convert.setIntValue(pageNo.pid, CUR_PAGE, data);
  }

  public PageId getNextPage() throws IOException {
    return new PageId(Convert.getIntValue(NEXT_PAGE, data));
  }

  public void setNextPage(PageId pageNo) throws IOException {
    Convert.setIntValue(pageNo.pid, NEXT_PAGE, data);
  }

  public PageId getPrevPage() throws IOException {
    return new PageId(Convert.getIntValue(PREV_PAGE, data));
  }

  public void setPrevPage(PageId pageNo) throws IOException {
    Convert.setIntValue(pageNo.pid, PREV_PAGE, data);
  }

  public byte[] getBMpageArray() throws Exception {
    byte[] bitMapArray = new byte[NUM_POSITIONS_IN_A_PAGE / 8];
    System.arraycopy(data, DPFIXED, bitMapArray, 0, bitMapArray.length);
    return bitMapArray;
  }

  void writeBMPageArray(byte[] givenData) throws Exception {
    System.arraycopy(givenData, 0, data, DPFIXED, givenData.length);
  }

  public Integer getCounter() throws Exception {
    return (int) Convert.getShortValue(COUNTER, data);
  }

  public void updateCounter(Short value) throws Exception {
    Convert.setShortValue(value, COUNTER, data);
    Convert.setShortValue((short) (NUM_POSITIONS_IN_A_PAGE - value * 8), FREE_SPACE, data);
  }

  private void clearSpace(int start, int length) {
    for (int i = start; i < start + length; i++) {
      data[i] = 0;
    }
  }
}
