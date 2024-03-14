package index;
import columnar.Columnarfile;

public class ColumnIndexScan {

    public ColumnIndexScan(Columnarfile cf, int columnNo, CondExpr[] selects, boolean indexOnly) {
        _selects = selects;
        index_only = indexOnly;
        try {

            columnarfile = cf;
            indName = columnarfile.getBTName(columnNo);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            btIndFile = new BTreeFile(indName);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
        }

        try {
            btIndScan = IndexUtils.BTree_scan(_selects, btIndFile);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
        }
    }
}
