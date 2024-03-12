package global;

/**
 * Enumeration class for AttrType
 * 
 */

public class AttrType {

  public static final int attrString  = 0;
  public static final int attrInteger = 1;
  public static final int attrReal    = 2;
  public static final int attrSymbol  = 3;
  public static final int attrNull    = 4;
  
  public int attrType;
  public int attrSize;

  /** 
   * AttrType Constructor
   * <br>
   * An attribute type of String can be defined as 
   * <ul>
   * <li>   AttrType attrType = new AttrType(AttrType.attrString);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (attrType.attrType == AttrType.attrString) ....
   * </ul>
   *
   * @param _attrType The types of attributes available in this class
   */

  public AttrType (int _attrType) {
    attrType = _attrType;
    switch(attrType) {
      case 0:
        attrSize = 20;
        break;
      case 1:
        attrSize = Integer.BYTES;
        break;
      case 2:
        attrSize = Float.BYTES;
        break;
      case 3:
        attrSize = 0;
        break;
      case 4:
        attrSize = 0;
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + attrType);
    }
  }

  public String toString() {

    switch (attrType) {
    case attrString:
      return "attrString";
    case attrInteger:
      return "attrInteger";
    case attrReal:
      return "attrReal";
    case attrSymbol:
      return "attrSymbol";
    case attrNull:
      return "attrNull";
    }
    return ("Unexpected AttrType " + attrType);
  }
}
