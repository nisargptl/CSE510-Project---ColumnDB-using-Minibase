javac -d out -cp src src/tests/Query.java
javac -d out -cp src src/tests/BatchInsert.java

rm -rf /tmp/name*
java -cp out tests.BatchInsert sample_2.txt name name 4 1
java -cp out tests.Query name name  name.1,name.2,name.3 "name.3 > 4" name.1,name.2 "FILE" "(name.1 = 5 v name.1 = 6),(name.2 > 7)" name.1,name.2,name.3 100 0

java -cp out tests.Index name name 3 1 BTREE
java -cp out tests.Index name name 3 1 BITMAP

java -cp out tests.Query name name name.3,name.4 "name.3 > 4" name.3,name.4 BTREE,BTREE "name.3 > 4" name.3,name.4 100 0
java -cp out tests.Query name name name.3,name.4 "name.3 > 4" name.3,name.4 BITMAP,BITMAP "name.3 > 4" name.3,name.4 100 0
