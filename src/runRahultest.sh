# javac -d out -cp src src/tests/Query.java
# echo "Sleeping for 5 seconds..."
# sleep 1
# # java -cp out tests.Query name name  name.1,name.2,name.3 "name.3 > 4" name.1,name.2 "FILE" "(name.1 = 5 v name.1 = 6),(name.2 > 7)" name.1,name.2,name.3 100 0
# java -cp out tests.Query name name  name.2,name.3 "name.3 > 4" name.1,name.2 "COLUMN" "(name.1 = 5 v name.1 = 6),(name.2 > 7)" name.2,name.3 100 0



# javac -d out -cp src src/tests/Query.java
# java -cp out tests.Query name name name.3,name.4 "name.3 > 4" name.3,name.4 BTREE,BTREE "name.3 > 4" name.3,name.4 100 0

# java -cp out tests.Query name name  name.1,name.2,name.3,name.4 "" name.1,name.2,name.3,name.4 "FILE" "" name.1,name.2,name.3,name.4 100 0
# java -cp out tests.Delete name name  name.1,name.2,name.3 "" name.1,name.2 "FILE" "" name.1,name.2,name.3 100 PURGE   



# javac -d out -cp src src/tests/BatchInsert.java
# rm -rf /tmp/name*
# java -cp out tests.BatchInsert sample_2.txt name name 4 1



javac -d out -cp src src/tests/Query.java
echo "Sleeping for 5 seconds..."
sleep 1
java -cp out tests.Query name name  name.1,name.2,name.3,name.4 " " name.1,name.2,name.3,name.4 "COLUMN" " " name.1,name.2,name.3,name.4 100 0

# javac -d out -cp src src/tests/ColumnSortDriver.java
# echo "Sleeping for 5 seconds..."
# sleep 1
# java -cp out tests.ColumnSortDriver name name  name.1,name.2,name.3,name.4 " " name.1,name.2,name.3,name.4 "FILE" " " name.1,name.2,name.3,name.4 100 0 2 0



# javac -d out -cp src src/tests/Delete.java
# echo "Sleeping for 5 seconds..."
# sleep 1
# java -cp out tests.Delete name name  name.1,name.2,name.3,name.4 " " name.1,name.2,name.3,name.4 "COLUMN" " " name.1,name.2,name.3,name.4 100 PURGE   



# javac -d out -cp src src/tests/BatchInsert.java

# rm -rf /tmp/name*
# java -cp out tests.BatchInsert sample_2.txt name name 4 1


# javac -d out -cp src src/tests/Query.java
# echo "Sleeping for 5 seconds..."
# sleep 1
# java -cp out tests.Query name name  name.1,name.2,name.3,name.4 "" name.1,name.2,name.3,name.4 "COLUMN" "" name.1,name.2,name.3,name.4 100 0

# java -cp out tests.Query name name  name.1,name.2,name.3,name.4 "name.3 > 0" name.1,name.2,name.3,name.4 "FILE" "name.3 > 0" name.1,name.2,name.3,name.4 100 0