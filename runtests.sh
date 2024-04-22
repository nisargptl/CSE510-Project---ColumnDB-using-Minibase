javac -d out -cp src src/tests/BatchInsert.java
javac -d out -cp src src/tests/Index.java
javac -d out -cp src src/tests/Query.java


rm -rf /tmp/name*
java -cp out tests.BatchInsert sample_2.txt name name 4 1

java -cp out tests.Index name name 3 1 BTREE
java -cp out tests.Index name name 3 1 BITMAP

java -cp out tests.Query name name name.3,name.4 "name.3 > 4" name.3,name.4 BTREE,BTREE "name.3 > 4" name.3,name.4 100 0
java -cp out tests.Query name name name.3,name.4 "name.3 > 4" name.3,name.4 BITMAP,BITMAP "name.3 > 4" name.3,name.4 100 0
java -cp out tests.Query name name  name.1,name.2,name.3 "name.3 > 4" name.1,name.2 "FILE" "(name.1 = 5 v name.1 = 6),(name.2 > 7)" name.1,name.2,name.3 100 0


java -cp out tests.Query name name name.3 "name.3 = 4" name.3 BTREE "name.3 = 4" name.3 100 0

java -cp out tests.Delete name name  name.1,name.2,name.3 "name.3 > 2" name.1,name.2 "FILE" "name.3 < 4" name.1,name.2,name.3 100 PURGE   


# for testing index scans
javac -d out -cp src src/tests/BatchInsert.java     
javac -d out -cp src src/tests/Query.java     
javac -d out -cp src src/tests/Index.java     
java -cp out tests.BatchInsert sample_2.txt name name 4 1
java -cp out tests.Index name name 3 BTREE
java -cp out tests.Index name name 4 BTREE
java -cp out tests.Query name name name.3,name.4 "name.3 = 4" name.3,name.4 BTREE,BTREE "name.3 = 4" name.3,name.4 100 0 # or BITMAP,BITMAP for bitmap index testing

# ColumnarDuplElimDriver:
java -cp out tests.Query name name  name.1,name.2,name.3,name.4 "name.3 > -1" name.1,name.2 "FILE" " " name.1,name.2,name.3,name.4 100 0
java -cp out tests.Query name name name.3 "name.3 = 4" name.3 BTREE "name.3 = 4" name.3 100 0
java BitmapEquiJoin COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST EQUICONST TARGETCOLUMNS NUMBUF


## BITMAP EQUIJOIN TEST COMMANDS
javac -d out -cp src src/tests/Index.java && javac -d out -cp src src/tests/Query.java && javac -d out -cp src src/tests/BatchInsert.java && javac -d out -cp src src/tests/BitMapEquiJoin.java && java -cp out tests.BatchInsert sample_2.txt name name 4 1 && java -cp out tests.Index name name 1 BITMAP && java -cp out tests.Index name name 2 BITMAP && java -cp out tests.Index name name 3 BITMAP && java -cp out tests.Index name name 4 BITMAP && java -cp out tests.Index name name 4 CBITMAP && java -cp out tests.Index name name 3 CBITMAP
java -cp out tests.Query name name name.3,name.4 "name.3 > 4" name.3,name.4 BITMAP,BITMAP "name.3 > 4" name.3,name.4 100 0
java -cp out tests.Query name name name.3,name.4 "name.3 > 4" name.3,name.4 CBITMAP,CBITMAP "name.3 > 4" name.3,name.4 100 0
java -cp out tests.BitMapEquiJoin name name name "" "" "name.1 = name.2" name.3,name.4 100 100
java -cp out tests.BitMapEquiJoin name name name "" "" "name.3 = name.4" name.3,name.4 100 100
