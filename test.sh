rm /tmp/name.minibase-db
rm -rf out
javac -d out -cp src src/tests/BatchInsert.java
java -cp out tests.BatchInsert sample_2.txt columnDB cf1 4 1
javac -d out -cp src src/tests/Index.java
java -cp out tests.Index columnDB cf1 1 BTREE
java -cp out tests.Index columnDB cf1 2 BTREE
java -cp out tests.Index columnDB cf1 3 BTREE
java -cp out tests.Index columnDB cf1 4 BTREE
java -cp out tests.Index columnDB cf1 4 BITMAP
java -cp out tests.Index columnDB cf1 3 BITMAP
java -cp out tests.Index columnDB cf1 2 BITMAP
java -cp out tests.Index columnDB cf1 1 BITMAP
javac -d out -cp src src/tests/Query.java
java -cp out tests.Query columnDB cf1  cf1.1,cf1.2,cf1.3,cf1.4 "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
java -cp out tests.Query columnDB cf1  cf1.1,cf1.2,cf1.3,cf1.4 "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2,cf1.3,cf1.4 "COLUMN" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
java -cp out tests.Query columnDB cf1  cf1.1,cf1.2 "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2 "COLUMN" " " cf1.1,cf1.2 100 0
java -cp out tests.Query columnDB cf1  cf1.1,cf1.2 "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2 "BTREE" " " cf1.1,cf1.2 100 0
java -cp out tests.Query columnDB cf1  cf1.1,cf1.2 "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2 "BTREE" "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2 100 0
java -cp out tests.Query columnDB cf1  cf1.1,cf1.2 "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2 "BITMAP" "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2 100 0
javac -d out -cp src src/tests/Query.java
java -cp out tests.Query columnDB cf1  cf1.1,cf1.2 "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2 "BTREE" "cf1.1 = 'New_Hampshire'" cf1.1,cf1.2 100 0
javac -d out -cp src src/tests/Delete.java
java -cp out tests.Delete columnDB cf1 cf1.1,cf1.3 "cf1.1 = 'Nevada'" cf1.3 FILE " " cf1.1,cf1.3 100 PU
#java -cp out tests.Query columnDB cf1  cf1.1,cf1.2 "cf1.1 = 'Nevada'" cf1.1,cf1.2 "BTREE" "cf1.1 = 'Nevada'" cf1.1,cf1.2 100 0
#java -cp out tests.Query columnDB cf1  cf1.1,cf1.2,cf1.3,cf1.4 "cf1.1 = 'Nevada'" cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0