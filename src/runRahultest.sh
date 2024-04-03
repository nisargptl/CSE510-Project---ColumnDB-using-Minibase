javac -d out -cp src src/tests/Query.java
echo "Sleeping for 5 seconds..."
sleep 1
# java -cp out tests.Query name name  name.1,name.2,name.3 "name.3 > 4" name.1,name.2 "FILE" "(name.1 = 5 v name.1 = 6),(name.2 > 7)" name.1,name.2,name.3 100 0
java -cp out tests.Query name name  name.2,name.3 "name.3 > 4" name.1,name.2 "COLUMN" "(name.1 = 5 v name.1 = 6),(name.2 > 7)" name.2,name.3 100 0