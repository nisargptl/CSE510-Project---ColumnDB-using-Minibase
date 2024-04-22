package bitmap;

import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

import java.io.IOException;

public class BitMapHeaderPage extends HFPage {

  // Constants for the offsets and sizes of various header fields
  private static final int DPFIXED = 4 * 2 + 3 * 4; // Assuming 4 short and 3 int fields in HFPage
  private static final int COLUMN_NUMBER_SIZE = 2;
  private static final int ATTR_TYPE_SIZE = 2;
  private static final int COLUMNAR_FILE_NAME_SIZE = 200;
  private static final int VALUE_SIZE = 400; // Assuming value is stored as a string
  private static final int COUNTER_SIZE = 4; // Size for an integer counter
  // Positions for each field within the data array
  private static final int COLUMN_NUMBER_POSITION = DPFIXED;
  private static final int ATTR_TYPE_POSITION =
          COLUMN_NUMBER_POSITION + COLUMN_NUMBER_SIZE;
  private static final int COLUMNAR_FILE_NAME_POSITION =
          ATTR_TYPE_POSITION + ATTR_TYPE_SIZE;
  private static final int VALUE_POSITION =
          COLUMNAR_FILE_NAME_POSITION + COLUMNAR_FILE_NAME_SIZE;
  private static final int COUNTER_POSITION = VALUE_POSITION + VALUE_SIZE; // Position immediately following the value


  /**
   * Constructor for creating a BitMapHeaderPage object from an existing page.
   *
   * @param pageno The PageId of the existing page.
   * @throws Exception Throws an exception if the page cannot be pinned.
   */
  public BitMapHeaderPage(PageId pageno) throws Exception {
    super();
    try {
      SystemDefs.JavabaseBM.pinPage(pageno, this, false);
    } catch (Exception e) {
      e.printStackTrace();
      throw new Exception("Unable to pin page: " + pageno.pid, e);
    }
  }

  /**
   * Default constructor for creating a new BitMapHeaderPage.
   *
   * @throws Exception Throws an exception if the page cannot be created.
   */
  public BitMapHeaderPage() throws Exception {
    super();
    try {
      PageId pageId = SystemDefs.JavabaseBM.newPage(this, 1);
      if (pageId == null) {
        throw new Exception("Failed to create a new page.");
      }
      this.init(pageId, this);
    } catch (Exception e) {
      e.printStackTrace();
      throw new Exception("Failed to initialize a new BitMapHeaderPage.", e);
    }
  }

  // Setters and Getters for the column number, attribute type, columnar file name, and value

  public void setColumnNumber(int columnNumber) throws Exception {
    Convert.setShortValue((short) columnNumber, COLUMN_NUMBER_POSITION, data);
  }

  public int getColumnNumber() throws Exception {
    return Convert.getShortValue(COLUMN_NUMBER_POSITION, data);
  }

  public void setAttrType(AttrType attrType) throws Exception {
    Convert.setShortValue((short) attrType.attrType, ATTR_TYPE_POSITION, data);
  }

  public AttrType getAttrType() throws Exception {
    short val = Convert.getShortValue(ATTR_TYPE_POSITION, data);
    return new AttrType(val);
  }

  public void setColumnarFileName(String columnName) throws Exception {
    Convert.setStrValue(columnName, COLUMNAR_FILE_NAME_POSITION, data);
  }

  public String getColumnarFileName() throws Exception {
    return Convert
            .getStrValue(COLUMNAR_FILE_NAME_POSITION, data, COLUMNAR_FILE_NAME_SIZE)
            .trim();
  }

  public void setValue(String value) throws Exception {
    Convert.setStrValue(value, VALUE_POSITION, data);
  }

  public String getValue() throws Exception {
    return Convert.getStrValue(VALUE_POSITION, data, VALUE_SIZE).trim();
  }

  // Methods for managing the root ID of the bitmap pages
  public void set_rootId(PageId rootID) throws Exception {
    setNextPage(rootID);
  }

  public PageId get_rootId() throws Exception {
    return getNextPage();
  }

  /***
   * getter for page id
   * @return
   * @throws IOException
   */
  public PageId getPageId() throws IOException {
    return getCurPage();
  }

  public void setCounter(int counter) throws IOException {
    Convert.setIntValue(counter, COUNTER_POSITION, data);
  }

  public int getCounter() throws IOException {
    return Convert.getIntValue(COUNTER_POSITION, data);
  }
}