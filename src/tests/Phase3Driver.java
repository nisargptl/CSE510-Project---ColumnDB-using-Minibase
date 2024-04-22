package tests;

import java.io.*;
import java.util.*;
import java.lang.*;

public class Phase3Driver {
    public static void main(String args[]) {
        System.out.println("1. Insert Data");
        System.out.println("2. Create index on a column");
        System.out.println("3. Query data in a columnar file");
        System.out.println("4. Perform deletion on a columnar file");
        System.out.println("5. Perform Nested Loop Join operation");
        System.out.println("6. Perform Index EquiJoin operation");
        System.out.println("7. Sort the columnarfile on a column");
        System.out.println("8. Delete duplicates in a columnarfile");

        System.out.println("Choose one from above: ");
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int choice = -1;
        try {
            choice = Integer.parseInt(in.readLine());
        }
        catch (NumberFormatException e) {
            System.out.println("NumberFormatException thrown at getInput!");
        }
        catch (IOException e) {
            System.out.println("IOException thrown at getInput!");
        }

        // switch (choice) {
        //     case 1:
            
        // }
    }
}