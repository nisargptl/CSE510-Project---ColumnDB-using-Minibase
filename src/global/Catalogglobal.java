package global;

import bufmgr.*;
import diskmgr.*;
import catalog.*;

public interface Catalogglobal {
  
   int MINIBASE_MAXARRSIZE = 50;

   // Global constants defined in CATALOG

   String RELCATNAME = "relcat";  // name of relation catalog
   String ATTRCATNAME = "attrcat"; // name of attribute catalog
   String INDEXCATNAME = "indcat";
   String RELNAME = "relname"; // name of indexed field in rel/attrcat
   int MAXNAME = 32; // length of relName, attrName
   int MAXSTRINGLEN = 255; // max. length of string attribute
   int NUMTUPLESFILE = 100; // default statistic = no recs in file
   int NUMPAGESFILE = 20; // default statistic = no pages in file
   int DISTINCTKEYS = 20; // default statistic: no of distinct keys
   int INDEXPAGES = 5; // default statisitc no of index pages
   String MINSTRINGVAL = "A"; // default statisitic
   String MAXSTRINGVAL = "ZZZZZZZ"; // default statisitic
   int MINNUMVAL = 0;
   int MAXNUMVAL = 999999999;
   
}

