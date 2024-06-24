<!--
Copyright (c) 2021 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on Redis with RediSearch[https://github.com/RediSearch/RediSearch] module enabled. 

### 1. Start Redis with RediSearch 

```
(...)
```

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/RediSearch/YCSB.git --branch redisearch2.support
    cd YCSB
    mvn -pl site.ycsb:redisearch-binding -am clean package

### 4. Provide Redis Connection Parameters
    
Set host, port, password, and cluster mode in the workload you plan to run. 

- `redisearch.host`
- `redisearch.port`
- `redisearch.password`
  * Don't set the password if redis auth is disabled.
- `redisearch.cluster`
  * Set the cluster parameter to `true` if redis cluster mode is enabled.
  * Default is `false`.

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load redisearch -s -P workloads/workloada -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=8" > outputLoad.txt


### 5. Load data and run the tests

All six workloads have a data set that is similar. Workloads D and E insert records during the test run. Thus, to keep the database size consistent, we recommend the following sequence:

Load the database, using workload A’s parameter file (workloads/workloada) and the “-load” switch to the client.

- Run workload A (using workloads/workloada and “-t”) for a variety of throughputs.

- Run workload B (using workloads/workloadb and “-t”) for a variety of throughputs.

- Run workload C (using workloads/workloadc and “-t”) for a variety of throughputs.

- Run workload F (using workloads/workloadf and “-t”) for a variety of throughputs.

- Run workload D (using workloads/workloadd and “-t”) for a variety of throughputs. This workload inserts records, increasing the size of the database.

- Delete the data in the database.

- Reload the database, using workload E’s parameter file (workloads/workloade) and the "-load switch to the client.

- Run workload E (using workloads/workloade and “-t”) for a variety of throughputs. This workload inserts records, increasing the size of the database.


```bash
# load, run A, B, C, F, D, (flushdb), load, E
./bin/ycsb load redisearch -s -P workloads/workloada -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=8" > outputLoad.txt

./bin/ycsb run redisearch -s -P workloads/workloada -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=8" > outputRunA.txt
./bin/ycsb run redisearch -s -P workloads/workloadb -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=8" > outputRunB.txt
./bin/ycsb run redisearch -s -P workloads/workloadc -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=8" > outputRunC.txt
./bin/ycsb run redisearch -s -P workloads/workloadf -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=8" > outputRunF.txt
./bin/ycsb run redisearch -s -P workloads/workloadd -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=8" > outputRunD.txt

redis-cli flushall

./bin/ycsb load redisearch -s -P workloads/workloade -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=32" -p "recordcount=30000000" -p "operationcount=30000000" > outputLoad.txt
./bin/ycsb run redisearch -s -P workloads/workloade -p "redisearch.host=127.0.0.1" -p "redisearch.port=6379" -p "threadcount=32" -p "recordcount=30000000" -p "operationcount=30000000" > outputRunE.txt
```


## An overall picture, and the RediSearch use-case

### Default workloads

By default, there are 6 default independent workloads, in the core package. Prior to describing the workloads, we will describe the dataset that is similar across all six.

#### RediSearch default workloads dataset representation

All six workloads have a data set that is similar, composed for N distinct records.

Each record is identified by a primary key, which is a string like “user234123”.


Within each record, by default, we have 10 fields, named field0, field1 and so on. The values of each field are a random string of ASCII characters of length L.
By default, we construct 1,000-byte records by using F = 10 fields, each of L = 100 bytes.

Specifically, within Redis, we model each record as a HASH, as showcased below.

Apart from the default fields, within each document we add a numeric `__score__` field, used within the `scan()` workloads for sorting. 
The value of the `__score__` field is given by computing a hash of the key name.


```bash
127.0.0.1:6379> hgetall user6873002678636213555
 1) "field6"
 2) "1;x.H{=8`73<+H71+t6W=99\"8(f>Tm*;<4K16?r#?:'<d.!<7\"6!I#'_=(M3!-(-6n>Q99:>8)< P+ 1x2\\%/M\x7f'W%%Sy\"^u%?n+"
 3) "field1"
 4) ")C3,U!6@-(G!:V5<<d9)j#\\+9&:5S#\":d;&v?>h;<&3)|&\".2< <O{%(:<R90\">4[s-Y'84 '!61%.=(.9R{9Aw*Cc-H=$H'*!,,"
 5) "field7"
 6) ".(v#Uq'2>=@5%E),*`9]',V->1.9'*&@s6L7:>>,Js:V)17:$Y58^-(Qk?Aq6 r\"Zm Ee&Ag%S=)I%6),)Jy ),=9r5O+%Ug)=x+"
 7) "field9"
 8) " &f2R).A;43f>Vq;3,<W/9$d#G=,\"4*H%)-(>30 7&4 p;)(=/<<Ny(6~670,(*4N''Ze&&2:P5$Vm728<Ks0 4!C?3;:48>'R}*"
 9) "field4"
10) "4)t&S+08$) f V1)Qm,>,,;6&2`\"!n=Wu#V)4-.=I;5@q+U+$Gq.Ou1H{3Ma<[k.X} .:![\x7f,..8C;\"Ns:Vy<8t6B1<Uq\"E)\" 0$"
11) "field8"
12) "8H+ 9d\"^/%968!|;2|+Q-=;f9?`&T%%Sc;>06Bk*]%=U',)8&W5-D+)-*=B)9&p2T}-^'5Kq%(8-K5634 *$)E=1.`+]{?Vi##l+"
13) "field5"
14) "\"Sa/W\x7f5&j6C1:5j:R-$8: =(5)&94b(,r6L')+h+G=!8b7T95#>$=:=%t;U?;]+-%(+Yu7P'7#:9<>=Zi?Ok&%<()p&.&-M5<Ow>"
15) "field3"
16) "+H9-8t>5|;]}22|&>x$T38,4#\"f3#00\\30R71.*!8n,_i'>:,-f\"Sk:Q9% $?Fa:Q%:E',S;0.h3[);J-$G\x7f-8x\"]'?V!$50>,f6"
17) "field0"
18) "23,!Za?Uq?T5-9<=<. B)!2$?$4</h=?$$2`:$x7T%'':<L%+, -]e3^m?\"`16.!I{1.j6R}\"/:<\"4;Dm\"/$>.`6Vs;W7(>l/7z#"
19) "field2"
20) "5Dk'Qw==04$*9,b;B+!])$7h44(-8~?H'1!t>D9%?$>5x3Z/%Ek6Kg<H+()6,\".8\"~7W!(F=&7 (Vc4]q#Im7?\"50 ,Qk6Zs4#n:"
21) "__score__"
22) "1.770379916E9"
```

#### Scanning based on RediSearch secondary index…

As you will see below, there is the need to model a scan operation within different records, in which we scan records in order, starting at a randomly chosen record key. 
The number of records to scan is randomly chosen. 

 To model the scan operation within RedisSearch, we use FT.SEARCH and use computed hash score from the key name as the lower limit
for the search query, and set +inf as the upper limit of the search result.

 The provided record count is passed via the LIMIT 0 <recordcound> FT.SEARCH argument.

 Together, the above FT.SEARCH command arguments fully comply with a sorted, randomly chosen starting key, with
 variadic record count replies.
 
Example FT.SEARCH command that a scan operation would generate.

```bash
127.0.0.1:6379> "FT.SEARCH" "index" "@__score__:[7.39074446E8 inf]" "LIMIT" "0" "13" "RETURN" "10" "field0" "field1" "field2" "field3" "field4" "field5" "field6" "field7" "field8" "field9"
 1) (integer) 350
 2) "user3232700585171816769"
 3)  1) "field0"
     2) "6;l+P=;R'8F/:Vi77:,S+-5j&8|?,,,0(%,x9_#%?z5+l/4*?1 -'&)@!$8n=^w+Js*)t;S/,<(4J#4=:=8x$[w;X) J#4 r9T90"
     3) "field1"
     4) "<4.5]u'Gm/!\"3Bs#%d!U%(Z)1Z-, h3?r'/6 581%x3A55Qs*B34%~ ) )^w-O=0Ok=D=1?4?%v+5(9]+(T=!-b\" x#@o\">6\"$p7"
     (...)
     (...)
    19) "field9"
    20) "8\"d2Dw7Z)8L?(D75820V?4;h*-*8,4:V+8!8:+>9,(05r3F!=(r2I#)D+)Xm5!*-Xa:O7-987Ti?Ce(8(=]' Mm+Y)*S%5K#7(00"
 4) "user1000385178204227360"
 5)  1) "field0"
     2) "'1r;A+(Sc#&:4-x:7|6D-(Z}2<0%66>$ 85v(/84Wo3'\"=3.!0p*+2%#,<-f9+|?7p#Y{/60*5|\"R1/Qk\">z##68F/-*<)*`+Xe*"
     (...)
     (...)
(...)
(...)
```
  

### Workload operation details

#### Workload A: Update heavy workload

- read/scan/update/insert ratio: 50/0/50/0
- Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
- Request distribution: Zipfian (some items are more popular than others, according to a Zipfian distribution)
- Workload: Core ( site.ycsb.workloads.CoreWorkload )
- Application example: Session store recording recent actions

#### Workload B: Read mostly workload

- read/scan/update/insert ratio: 95/0/5/0

- Default data size: 1 KB records (10 fields, 100 bytes each, plus key)

- Request distribution: Zipfian (some items are more popular than others, according to a Zipfian distribution)

- Workload: Core ( site.ycsb.workloads.CoreWorkload )

- Application example: photo tagging; add a tag is an update, but most operations are to read tags

#### Workload C: Read only

- read/scan/update/insert ratio: 100/0/0/0

- Default data size: 1 KB records (10 fields, 100 bytes each, plus key)

- Request distribution: Zipfian (some items are more popular than others, according to a Zipfian distribution)

- Workload: Core ( site.ycsb.workloads.CoreWorkload )

- Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)

#### Workload D: Read latest workload

- read/scan/update/insert ratio: 95/0/0/5

- Default data size: 1 KB records (10 fields, 100 bytes each, plus key)

- Request distribution: latest -- new records are inserted, and the most recently inserted records are the most popular.

- Workload: Core ( site.ycsb.workloads.CoreWorkload )

- Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)

#### Workload E: Short ranges

- read/scan/update/insert ratio: 0/95/0/5

- maxscanlength=100

- scanlengthdistribution=uniform

- This is a 95% read workload, in which short ranges of records are queried, instead of individual records.

- Default data size: 1 KB records (10 fields, 100 bytes each, plus key)

- Request distribution: Zipfian (some items are more popular than others, according to a Zipfian distribution)

- Workload: Core ( site.ycsb.workloads.CoreWorkload )

- Application example: threaded conversations, where each scan is for the posts in a given thread (assumed to be clustered by thread id)

#### Workload F: Read-modify-write workload

- read/scan/update/insert/readmodifywrite ratio: 50/0/0/0/50

- Default data size: 1 KB records (10 fields, 100 bytes each, plus key)

- Request distribution: Zipfian (some items are more popular than others, according to a Zipfian distribution)

- Workload: Core ( site.ycsb.workloads.CoreWorkload )

- Application example: user database, where user records are read and modified by the user or to record user activity.
