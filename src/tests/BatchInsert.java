package tests;

import bufmgr.*;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static global.GlobalConst.NUMBUF;

public class BatchInsert {
    public static int NUM_PAGES = 10000;

    public static void main(String[] args) throws IOException, HashOperationException, PageNotFoundException, BufMgrException, PagePinnedException, PageUnpinnedException {
        String dataFileName = args[0];
        String columnDBName = args[1];
        String columnarFileName = args[2];
        Integer numColumns = Integer.parseInt(args[3]);
        Integer isNewDb = Integer.parseInt(args[4]);

        int numPages = isNewDb == 1 ? NUM_PAGES : 0;

        String dbpath = OperationUtils.dbPath(columnDBName);
        SystemDefs sysdef = new SystemDefs(dbpath, numPages, NUMBUF, "Clock");

        runOperation(dataFileName, columnarFileName, numColumns);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runOperation(String dataFileName, String columnarFile, int numColumns) throws IOException {
        FileInputStream fileStream = null;
        BufferedReader b = null;
        try {
            fileStream = new FileInputStream(dataFileName);
            b = new BufferedReader(new InputStreamReader(fileStream));

            AttrType[] attrTypes = new AttrType[numColumns];
            short[] attrSizes = new short[numColumns];
            String[] names = new String[numColumns];
            String record;

            // First line in the data file has attr type info. Read it and split it on tab spaces to get individual columns
            String attrType = b.readLine();
            String[] parts = attrType.split("\t");

            int i = 0;
            for (String s : parts) {
                String[] t = s.split(":");
                names[i] = t[0];
                if (t[1].contains("char")) {
                    attrTypes[i] = new AttrType(AttrType.attrString);
                    attrSizes[i] = Short.parseShort(t[1].substring(5, t[1].length() - 1));
                    i++;
                } else {
                    attrTypes[i] = new AttrType(AttrType.attrInteger);
                    attrSizes[i] = 4;
                    i++;
                }
            }

            // Create a columnar file for the data to be inserted using the type information parsed above
            Columnarfile cf = new Columnarfile(columnarFile, numColumns, attrTypes, attrSizes);

            int counter = 0;

            //  Start inserting the data
            while ((record = b.readLine()) != null) {
                String[] vals = record.split("\t");

                Tuple t = new Tuple();
                t.setHdr((short) numColumns, attrTypes, attrSizes);
                int size = t.size();

                t = new Tuple(size);
                t.setHdr((short) numColumns, attrTypes, attrSizes);
                int j = 0;
                for (String val : vals) {
                    System.out.println(val);
                    switch (attrTypes[j].attrType) {
                        case 0:
                            t.setStrFld(j + 1, val);
                            j++;
                            break;
                        case 1:
                            t.setIntFld(j + 1, Integer.parseInt(val));
                            j++;
                            break;
                        default:
                            j++;
                            break;
                    }
                }
                cf.insertTuple(t.getTupleByteArray());
                counter++;
            }

            b.close();
            System.out.println(counter + " tuples inserted in columnar file : " + columnarFile);
            System.out.println();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileStream.close();
            b.close();
        }
    }
}
