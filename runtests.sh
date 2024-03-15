javac -d out -cp src src/operations/Query.java
javac -d out -cp src src/operations/BatchInsert.java

rm -rf /tmp/name*
java -cp out operations.BatchInsert sample_2.txt name name 4 1
java -cp out operations.Query name name  name.1,name.2,name.3 "name.3 > 4" name.1,name.2 "FILE" "(name.1 = 5 v name.1 = 6),(name.2 > 7)" name.1,name.2,name.3 100 0

