: 1714110098:0;rm -rf out
: 1714110120:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714110125:0;ls /tmp
: 1714110157:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714110238:0;java -cp out tests.BatchInsert sample_3.txt columnDB cf1 4 1
: 1714110243:0;java -cp out tests.BatchInsert sample_3.txt columnDB cf1 5 1
: 1714110246:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714110308:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714110311:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714110327:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714110329:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714110739:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714110742:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714110770:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714110773:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714110873:0;java -cp out tests.BatchInsert sample_3.txt columnDB cf1 4 1
: 1714110883:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714110887:0;java -cp out tests.BatchInsert sample_3.txt columnDB cf1 4 1
: 1714110895:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714110897:0;java -cp out tests.BatchInsert sample_3.txt columnDB cf1 4 1
: 1714110920:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714110927:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714110985:0;javac -d out -cp src src/tests/Index.java
: 1714111009:0;java -cp out tests.Index
: 1714111014:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714111027:0;java -cp out tests.Index cf1 BTREE 1
: 1714111035:0;java -cp out tests.Index cf1 1 BTREE
: 1714111082:0;java -cp out tests.Index columnDB cf1 1 BTREE
: 1714111086:0;java -cp out tests.Index columnDB cf1 2 BTREE
: 1714111089:0;java -cp out tests.Index columnDB cf1 3 BTREE
: 1714111091:0;java -cp out tests.Index columnDB cf1 4 BTREE
: 1714111093:0;java -cp out tests.Index columnDB cf1 5 BTREE
: 1714111102:0;java -cp out tests.Index columnDB cf1 0 BTREE
: 1714111124:0;java -cp out tests.Index columnDB cf1 1 BITMAP
: 1714111127:0;java -cp out tests.Index columnDB cf1 1 CBITMAP
: 1714111130:0;java -cp out tests.Index columnDB cf1 2 CBITMAP
: 1714111154:0;java -cp out tests.Index columnDB cf1 2 BITMAP
: 1714111182:0;rm /tmp/columnDB.minibase-db
: 1714111187:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714111193:0;java -cp out tests.Index columnDB cf1 1 BITMAP
: 1714111202:0;java -cp out tests.Index columnDB cf1 1 BTREE
: 1714111210:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714111229:0;javac -d out -cp src src/tests/Query.java
: 1714111276:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 "FILE" " " name.3,name.4 100 0
: 1714111416:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 "BTREE" "cf1 < 5" cf1.3,cf1.4 100 0
: 1714111422:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 "BTREE" "cf1 > 5" cf1.3,cf1.4 100 0
: 1714111430:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 "FILE" " " name.3,name.4 100 0
: 1714111439:0;java -cp out tests.Index columnDB cf1 1 BTREE
: 1714111441:0;java -cp out tests.Index columnDB cf1 2 BTREE
: 1714111444:0;java -cp out tests.Index columnDB cf1 3 BTREE
: 1714111446:0;java -cp out tests.Index columnDB cf1 4 BTREE
: 1714111448:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 "FILE" " " name.3,name.4 100 0
: 1714111472:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 " " cf1.3,cf1.4 "BTREE" "cf1 > 4" cf1.3,cf1.4 100 0
: 1714111479:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 "FILE" " " name.3,name.4 100 0
: 1714111482:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 " " cf1.3,cf1.4 "BTREE" "cf1 > 4" cf1.3,cf1.4 100 0
: 1714111496:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 " " cf1.3,cf1.4 "BTREE" "cf1.3 > 4" cf1.3,cf1.4 100 0
: 1714111508:0;java -cp out tests.Index columnDB cf1 4 BTREE
: 1714111511:0;java -cp out tests.Index columnDB cf1 2 BTREE
: 1714111513:0;java -cp out tests.Index columnDB cf1 3 BTREE
: 1714111531:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 0" cf1.3,cf1.4 "BTREE" "cf1.3 > 4" cf1.3,cf1.4 100 0
: 1714111545:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 0" cf1.3,cf1.4 BTREE "cf1.3 > 4" cf1.3,cf1.4 100 0
: 1714111553:0;rm /tmp/columnDB.minibase-db
: 1714111559:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714111561:0;java -cp out tests.Index columnDB cf1 3 BTREE
: 1714111564:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 0" cf1.3,cf1.4 BTREE "cf1.3 > 4" cf1.3,cf1.4 100 0
: 1714111579:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 "FILE" " " name.3,name.4 100 0
: 1714111593:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 BTREE "cf1.3 > 4" cf1.3,cf1.4 100 0
: 1714111619:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4" cf1.3,cf1.4 BTREE " " cf1.3,cf1.4 100 0
: 1714111670:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 4 and cf1.1 = 'Alabama'" cf1.3,cf1.4 "FILE" " " name.3,name.4 100 0
: 1714111702:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 > 4 and cf1.1 = 'Alabama'" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
: 1714111714:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 > 0 and cf1.1 = 'Alabama'" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
: 1714111719:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 < 0 and cf1.1 = 'Alabama'" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
: 1714111756:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 > 2 and cf1.3 < 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
: 1714111771:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 > 2 and cf1.3 < 8" cf1.3,cf1.4 "BTREE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
: 1714111824:0;javac -d out -cp src src/tests/Delete.java
: 1714111834:0;java -d out -cp src src/tests/Delete.java
: 1714111838:0;javac -d out -cp src src/tests/Delete.java
: 1714111918:0;java -cp out tests.Delete columnDB cf1 cf1.1,cf1.2,cf1.3 "cf1.3 > 8" cf1.1,cf1.2 "FILE" " " cf1.1,cf1.2,cf1.3 100 PURGE
: 1714111942:0;java -cp out tests.Delete columnDB cf1 cf1.1,cf1.2,cf1.3 "cf1.3 < 8 and cf1.3 > 6" cf1.1,cf1.2 "FILE" " " cf1.1,cf1.2,cf1.3 100 PU
: 1714111946:0;java -cp out tests.Delete columnDB cf1 cf1.1,cf1.2,cf1.3 "cf1.3 < 8 and cf1.3 > 6" cf1.1,cf1.2 "FILE" " " cf1.1,cf1.2,cf1.3 100 PURGE
: 1714111966:0;java -cp out tests.Delete columnDB cf1 cf1.1,cf1.2,cf1.3 "cf1.3 < 8 and cf1.3 > 4" cf1.1,cf1.2 "BTREE" " " cf1.1,cf1.2,cf1.3 100 PU
: 1714111975:0;java -cp out tests.Delete columnDB cf1 cf1.1,cf1.2,cf1.3 "cf1.3 < 8 and cf1.3 > 4" cf1.1,cf1.2 BTREE " " cf1.1,cf1.2,cf1.3 100 PU
: 1714112145:0;git log
: 1714112198:0;git diff 2379eef5924eb107c778ce16150cf1b20f0dcaab
: 1714112297:0;java -cp out tests.BatchInsert sample_5.txt columnDB cf2 5 0
: 1714112352:0;javac -d out -cp src src/tests/ColumnarNestedLoopJoinDriver.java
: 1714112361:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf2.cf2.1,cf2.2.2,cf2.cf2.3" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3" "cf1.cf1.3 = cf2.cf2.3" 20 100
: 1714112390:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf2.cf2.1,cf2.2.2,cf2.cf2.3" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3" "cf1.cf1.3 = cf2.cf2.3 and cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112406:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf2.cf2.1,cf2.2.2,cf2.cf2.3" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112468:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1,cf1,4" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112484:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1,.cf1.4" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112488:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112500:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112581:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 0" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112598:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112630:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 0" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 0" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.3" 20 100
: 1714112640:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 0" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 0" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112645:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 4" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.4" 20 100
: 1714112648:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 0" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 0" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.3 = cf2.cf2.3" 20 100
: 1714112658:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 0" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 0" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.2 = cf2.cf2.1" 20 100
: 1714112680:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 0" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 0" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.2 = cf2.cf2.2" 20 100
: 1714112685:0;java -cp out tests.ColumnarNestedLoopJoinDriver columnDB cf1 cf1 "cf1.cf1.1, cf1.cf1.2,cf1.cf1.3,cf1.cf1.4,cf2.cf2.1,cf2.2.2,cf2.cf2.3,cf1.cf1.4" "cf1.cf1.3 > 4" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "FILE" "cf1.cf1.3 > 0" "cf1.cf1.1,cf1.cf1.2,cf1.cf1.3,cf1.cf1.4" "cf2.cf2.3 > 0" "cf2.cf2.1,cf2.cf2.2,cf2.cf2.3,cf2.cf2.4" "cf1.cf1.2 = cf2.cf2.1" 20 100
: 1714112736:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 > 2 and cf1.3 < 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
: 1714112762:0;java -cp out tests.Query columnDB cf2 cf2.1,cf2.2,cf2.3,cf2.4 "cf2.3 > 2 and cf2.3 < 8" cf2.3,cf2.4 "FILE" " " cf2.1,cf2.2,cf2.3,cf2.4 100 0
: 1714112791:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 > 2 and cf1.3 < 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 0
: 1714112802:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 > 2 and cf1.3 < 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714112809:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 "cf1.3 > 2 and cf1.3 < 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714112839:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.5 = 'Camel'" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714112867:0;java -cp out tests.Query columnDB cf2 cf2.1,cf2.2,cf2.3,cf2.4 "cf2.5 = 'Camel'" cf2.3,cf2.4 "FILE" " " cf2.1,cf2.2,cf2.3,cf2.4 100 0
: 1714112884:0;java -cp out tests.Query columnDB cf2 cf2.1,cf2.2,cf2.3,cf2.4 "cf2.5 = 'Camel'" cf2.3,cf2.4 "FILE" " " cf2.1,cf2.2,cf2.3,cf2.4,cf2.5 100 0
: 1714112928:0;java -cp out tests.Query columnDB cf2 cf2.1,cf2.2,cf2.3,cf2.4,cf2.5 "cf2.5 = 'Camel'" cf2.3,cf2.4,cf2.5 "FILE" " " cf2.1,cf2.2,cf2.3,cf2.4,cf2.5 100 0
: 1714112969:0;rm /tmp/columnDB.minibase-db
: 1714112982:0;java -cp out tests.BatchInsert sample_5.txt columnDB cf1 5 1
: 1714112988:0;java -cp out tests.Query columnDB cf2 cf2.1,cf2.2,cf2.3,cf2.4,cf2.5 "cf2.5 = 'Camel'" cf2.3,cf2.4,cf2.5 "FILE" " " cf2.1,cf2.2,cf2.3,cf2.4,cf2.5 100 0
: 1714113000:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.5 = 'Camel'" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714113038:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 "cf1.3 > 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714113046:0;java -cp out tests.Query columnDB cf1 cf1.1,cf1.2,cf1.3,cf1.4 "cf1.3 > 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714113062:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4 "cf1.3 > 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714113068:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4,cf1.5 "cf1.3 > 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714113168:0;rm out
: 1714113172:0;rm -rf out
: 1714113184:0;javac -d out -cp src src/tests/Query.java
: 1714113188:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4,cf1.5 "cf1.3 > 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714113197:0;rm /tmp/columnDB.minibase-db
: 1714113206:0;javac -d out -cp src src/tests/BatchInsert.java
: 1714113216:0;java -cp out tests.BatchInsert sample_5.txt columnDB cf1 5 1
: 1714113219:0;java -cp out tests.Query columnDB cf1 cf1.3,cf1.4,cf1.5 "cf1.3 > 8" cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4,cf1.5 100 0
: 1714113316:0;javac -d out -cp src src/tests/ColumnSortDriver.java
: 1714113542:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 100 3 0 100
: 1714113547:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 100 3 0 200
: 1714113549:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100 100 3 0 2000
: 1714113553:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 1000 1000 3 0 2000
: 1714113616:0;javac -d out -cp src src/tests/ColumnSortDriver.java
: 1714113620:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 1000 1000 3 0 2000
: 1714113652:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100000 100000 3 0 200000
: 1714113661:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 10000000 100000000 3 0 20000000000
: 1714113665:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 10000000 100000000 3 0 2000000000
: 1714113674:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 10000000 100000000 3 0 200000000
: 1714113680:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 10000000 100000000 3 0 2000000
: 1714113691:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 100000 100000 3 0 2000000
: 1714113699:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 1000 1000 3 0 200
: 1714113730:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714113733:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 1000 1000 3 0 200
: 1714113741:0;rm /tmp/columnDB.minibase-db
: 1714113743:0;java -cp out tests.BatchInsert sample_4.txt columnDB cf1 5 1
: 1714113748:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.1,cf1.2,cf1.3 " " cf1.1,cf1.2,cf1.3,cf1.4 "FILE" " " cf1.1,cf1.2,cf1.3,cf1.4 1000 1000 3 0 200
: 1714113775:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.2,cf1.3 " " cf1.2,cf1.3 "FILE" " " cf1.2,cf1.3 1000 1000 3 0 200
: 1714113797:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.2,cf1.3 " " cf1.2,cf1.3 "FILE" " " cf1.2,cf1.3 1000 1000 1 0 200
: 1714113885:0;rm /tmp/columnDB.minibase-db
: 1714113890:0;java -cp out tests.BatchInsert sample_3.txt columnDB cf1 5 1
: 1714113893:0;java -cp out tests.BatchInsert sample_3.txt columnDB cf1 4 1
: 1714113897:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.2,cf1.3 " " cf1.2,cf1.3 "FILE" " " cf1.2,cf1.3 1000 1000 1 0 200
: 1714113970:0;rm -rf out
: 1714113992:0;javac -d out -cp src src/tests/ColumnSortDriver.java
: 1714113996:0;java -cp out tests.ColumnSortDriver columnDB cf1 cf1.2,cf1.3 " " cf1.2,cf1.3 "FILE" " " cf1.2,cf1.3 1000 1000 1 0 200
