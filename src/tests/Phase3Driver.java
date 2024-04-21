package tests;

import java.io.*;
import java.util.*;
import java.lang.*;

public class Phase3Driver {
    public static void main(String args[]) throws Exception {
        System.out.println("1. Insert Data");
        System.out.println("2. Create index on a column");
        System.out.println("3. Query data in a columnar file");
        System.out.println("4. Perform deletion on a columnar file");
        System.out.println("5. Perform Nested Loop Join operation");
        System.out.println("6. Perform Index EquiJoin operation");
        System.out.println("7. Sort the columnarfile on a column");
        System.out.println("8. Delete duplicates in a columnarfile");

        System.out.println("Choose one from above: ");
        BufferedReader reader = new BufferedReader (new InputStreamReader(System.in));
        int choice = -1;
        try {
            choice = Integer.parseInt(reader.readLine());
        }
        catch (NumberFormatException e) {
            System.out.println("NumberFormatException thrown at getInput!");
        }
        catch (IOException e) {
            System.out.println("IOException thrown at getInput!");
        }
        System.out.println(choice);
        switch (choice) {
                case 1:
                    System.out.println("Enter the following parameters separated by a space \"dataFileName columnDBName columnarFileName No._of_columns isNewDb\": ");
                    try{
                        String[] params;
                        params= reader.readLine().split(" ");
                        System.out.println("Data file name: " + params[0]);
                        System.out.println("Column DB name: " + params[1]);
                        System.out.println("Columnar file name: " + params[2]);
                        System.out.println("Number of columns: " + params[3]);
                        System.out.println("is new DB: " + params[4]);

                        BatchInsert.main(params);
                    }
                    catch (IOException e) {
                        System.out.println("IOException thrown at getInput!");
                    }
                    
                    break;
                case 2:
                    // Code for creating index on a column
                    System.out.println("Enter the following parameters separated by a space \"columndbname columnarfilename columnnumber indextype(BITMAP/BTREE)\": ");
                    try{
                        String[] params;
                        params= reader.readLine().split(" ");
                        System.out.println("Column DB name: " + params[0]);
                        System.out.println("Columnar file name: " + params[1]);
                        System.out.println("column number: " + params[2]);
                        System.out.println("Index type: " + params[3]);

                        Index.main(params);
                    }
                    catch (IOException e) {
                        System.out.println("IOException thrown at getInput!");
                    }
                    break;
                case 3:
                    // Code for querying data in a columnar file
                    System.out.println("Enter the following parameters separated by a space \"columndbname columnarfilename projection\": ");
                    try{
                        String[] params;
                        params = reader.readLine().split(" ");
                        System.out.println("Column DB name: " + params[0]);
                        System.out.println("Columnar file name: " + params[1]);
                        System.out.println("Projection: " + params[2]);
//                        constraint scancols scantypes scanconst targetcolumns numbuf sortmem
                        String constr, scancols, scantypes, scanconst, targetcols, numbuf, sortmem;
                        System.out.print("Enter Constraint: ");
                        constr = reader.readLine();
                        System.out.print("Enter the Scan Cols: ");
                        scancols = reader.readLine();
                        System.out.print("Enter Scan Type: ");
                        scantypes = reader.readLine();
                        System.out.print("Enter Scan constraint: ");
                        scanconst = reader.readLine();
                        System.out.print("Enter Target Columns: ");
                        targetcols = reader.readLine();
                        System.out.print("Enter Number of buffer frames: ");
                        numbuf = reader.readLine();
                        System.out.print("Enter Amount of sort memory: ");
                        sortmem = reader.readLine();
                        params = new String[]{params[0], params[1], params[2], constr, scancols, scantypes, scanconst, targetcols, numbuf, sortmem};

                        Query.main(params);
                    }
                    catch (IOException e) {
                        System.out.println("IOException thrown at getInput!");
                    }
                    break;
                case 4:
                    // Code for performing deletion on a columnar file
                    break;
                case 5:
                    // Code for performing Nested Loop Join operation
                    break;
                case 6:
                    // Code for performing Index EquiJoin operation
                    break;
                case 7:
                    // Code for sorting the columnarfile on a column
                    break;
                case 8:
                    // Code for deleting duplicates in a columnarfile
                    break;
                default:
                    System.out.println("Invalid choice!");
                    break;

        }
    }
}