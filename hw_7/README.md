
## 1. На основе архитектуры `Снежинка` из полученного ранее домашнего задания, составить запросы. Замерить время их выполнения:
### Запрос 1: Получение заказов клиентов с указанием сведений о заказе и клиент
```sql
EXPLAIN ANALYZE
SELECT c.c_custkey
     , c.c_name
     , c.c_address
     , c.c_phone
     , o.o_orderkey
     , o.o_orderdate
     , o.o_shippriority
  FROM customer AS c
  JOIN orders AS o ON o.o_custkey = c.c_custkey;
```
```sql
Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..1582.16 rows=1500000 width=81) (actual time=9.888..675.203 rows=1500000 loops=1)
  ->  Hash Join  (cost=0.00..1175.74 rows=375000 width=81) (actual time=8.971..356.497 rows=375274 loops=1)
        Hash Cond: (orders.o_custkey = customer.c_custkey)
        Extra Text: (seg1)   Hash chain length 1.1 avg, 4 max, using 34819 of 262144 buckets.
        ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..501.50 rows=375000 width=20) (actual time=0.013..214.788 rows=375274 loops=1)
              Hash Key: orders.o_custkey
              ->  Sequence  (cost=0.00..456.57 rows=375000 width=20) (actual time=3.110..183.481 rows=375552 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..456.57 rows=375000 width=20) (actual time=3.089..168.588 rows=375552 loops=1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
        ->  Hash  (cost=434.57..434.57 rows=37500 width=65) (actual time=9.380..9.380 rows=37551 loops=1)
              ->  Seq Scan on customer  (cost=0.00..434.57 rows=37500 width=65) (actual time=0.278..2.754 rows=37551 loops=1)
Planning time: 19.379 ms
  (slice0)    Executor memory: 560K bytes.
  (slice1)    Executor memory: 43925K bytes avg x 4 workers, 43931K bytes max (seg3).
  (slice2)    Executor memory: 10736K bytes avg x 4 workers, 10736K bytes max (seg0).  Work_mem: 3465K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 722.726 ms
```
### Запрос 2: Получение подробной информации о заказе с указанием позиций
```sql
EXPLAIN ANALYZE 
SELECT o.o_orderkey
     , o.o_orderdate
     , o.o_shippriority
     , l.l_linenumber
     , l.l_quantity
     , l.l_extendedprice
     , l.l_discount
  FROM orders AS o 
  JOIN lineitem AS l ON l.l_orderkey = o.o_orderkey;
```
```sql
Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..2686.89 rows=6084369 width=37) (actual time=417.873..1516.515 rows=6001215 loops=1)
  ->  Hash Join  (cost=0.00..1933.86 rows=1521093 width=37) (actual time=417.120..1361.279 rows=1503360 loops=1)
        Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
        Extra Text: (seg3)   Hash chain length 1.4 avg, 7 max, using 267768 of 524288 buckets.
        ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..802.19 rows=1500304 width=29) (actual time=0.023..255.708 rows=1503360 loops=1)
              Hash Key: lineitem.l_orderkey
              ->  Sequence  (cost=0.00..541.57 rows=1500304 width=29) (actual time=3.959..432.995 rows=1501915 loops=1)
                    ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..541.57 rows=1500304 width=29) (actual time=3.939..365.131 rows=1501915 loops=1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
        ->  Hash  (cost=456.57..456.57 rows=375000 width=16) (actual time=411.587..411.587 rows=375552 loops=1)
              ->  Sequence  (cost=0.00..456.57 rows=375000 width=16) (actual time=0.692..333.657 rows=375552 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..456.57 rows=375000 width=16) (actual time=0.680..226.690 rows=375552 loops=1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
Planning time: 24.894 ms
  (slice0)    Executor memory: 167K bytes.
  (slice1)    Executor memory: 60662K bytes avg x 4 workers, 60663K bytes max (seg0).
  (slice2)    Executor memory: 70978K bytes avg x 4 workers, 70982K bytes max (seg3).  Work_mem: 14670K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 1679.073 ms
```
### Запрос 3: Получение информации о поставщике и детали для каждого отношения Поставщик-деталь
```sql
EXPLAIN ANALYZE
SELECT s.s_suppkey
     , s.s_name
     , s.s_address
     , s.s_phone
     , ps.ps_partkey
     , p.p_name
     , p.p_retailprice
     , p.p_comment
  FROM supplier AS s
  JOIN partsupp AS ps ON ps.ps_suppkey = s.s_suppkey 
  JOIN part AS p ON p.p_partkey = ps.ps_partkey;
```
```sql
Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..1938.71 rows=800000 width=128) (actual time=57.381..435.993 rows=800000 loops=1)
  ->  Hash Join  (cost=0.00..1596.19 rows=200000 width=128) (actual time=56.777..245.574 rows=200372 loops=1)
        Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
        Extra Text: (seg2)   Hash chain length 1.0 avg, 3 max, using 9647 of 131072 buckets.
        ->  Hash Join  (cost=0.00..1005.96 rows=200000 width=61) (actual time=51.211..132.277 rows=200372 loops=1)
              Hash Cond: (part.p_partkey = partsupp.ps_partkey)
              Extra Text: (seg2)   Hash chain length 4.4 avg, 16 max, using 45643 of 262144 buckets.
              ->  Seq Scan on part  (cost=0.00..434.96 rows=50000 width=57) (actual time=0.149..7.166 rows=50093 loops=1)
              ->  Hash  (cost=456.25..456.25 rows=200000 width=8) (actual time=50.171..50.171 rows=200372 loops=1)
                    ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..456.25 rows=200000 width=8) (actual time=0.082..30.028 rows=200372 loops=1)
                          Hash Key: partsupp.ps_partkey
                          ->  Seq Scan on partsupp  (cost=0.00..448.27 rows=200000 width=8) (actual time=0.067..24.073 rows=200662 loops=1)
        ->  Hash  (cost=441.32..441.32 rows=10000 width=71) (actual time=5.683..5.683 rows=10000 loops=1)
              ->  Broadcast Motion 4:4  (slice2; segments: 4)  (cost=0.00..441.32 rows=10000 width=71) (actual time=1.125..3.751 rows=10000 loops=1)
                    ->  Seq Scan on supplier  (cost=0.00..431.22 rows=2500 width=71) (actual time=0.268..0.485 rows=2544 loops=1)
Planning time: 10.217 ms
  (slice0)    Executor memory: 1288K bytes.
  (slice1)    Executor memory: 284K bytes avg x 4 workers, 284K bytes max (seg0).
  (slice2)    Executor memory: 496K bytes avg x 4 workers, 496K bytes max (seg0).
  (slice3)    Executor memory: 22184K bytes avg x 4 workers, 22184K bytes max (seg0).  Work_mem: 6262K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 463.166 ms
```
### Запрос 4: Получение подробной информации о заказе клиента и позиции
```sql
EXPLAIN ANALYZE
SELECT c.c_custkey
     , c.c_name
     , c.c_address
     , c.c_phone
     , o.o_orderkey
     , o.o_orderdate
     , o.o_shippriority
     , l.l_linenumber
     , l.l_quantity
     , l.l_extendedprice
     , l.l_discount
  FROM customer AS c
  JOIN orders AS o ON o.o_custkey = c.c_custkey
  JOIN lineitem AS l ON l.l_orderkey = o.o_orderkey;
```
```sql
Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..5608.18 rows=6084369 width=102) (actual time=339.860..2477.527 rows=6001215 loops=1)
  ->  Hash Join  (cost=0.00..3532.25 rows=1521093 width=102) (actual time=339.401..1270.900 rows=1502518 loops=1)
        Hash Cond: (orders.o_custkey = customer.c_custkey)
        Extra Text: (seg1)   Hash chain length 1.2 avg, 5 max, using 32441 of 131072 buckets.
        ->  Redistribute Motion 4:4  (slice2; segments: 4)  (cost=0.00..2188.69 rows=1521093 width=41) (actual time=330.073..657.654 rows=1502518 loops=1)
              Hash Key: orders.o_custkey
              ->  Hash Join  (cost=0.00..1993.49 rows=1521093 width=41) (actual time=411.270..1426.372 rows=1503360 loops=1)
                    Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
                    Extra Text: (seg3)   Hash chain length 1.9 avg, 10 max, using 199647 of 262144 buckets.
                    ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..802.19 rows=1500304 width=29) (actual time=0.026..268.152 rows=1503360 loops=1)
                          Hash Key: lineitem.l_orderkey
                          ->  Sequence  (cost=0.00..541.57 rows=1500304 width=29) (actual time=2.692..500.166 rows=1501915 loops=1)
                                ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                                      Partitions selected: 87 (out of 87)
                                ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..541.57 rows=1500304 width=29) (actual time=2.682..434.005 rows=1501915 loops=1)
                                      Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
                    ->  Hash  (cost=456.57..456.57 rows=375000 width=20) (actual time=410.028..410.028 rows=375552 loops=1)
                          ->  Sequence  (cost=0.00..456.57 rows=375000 width=20) (actual time=1.877..252.180 rows=375552 loops=1)
                                ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                                      Partitions selected: 87 (out of 87)
                                ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..456.57 rows=375000 width=20) (actual time=1.866..137.914 rows=375552 loops=1)
                                      Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
        ->  Hash  (cost=434.57..434.57 rows=37500 width=65) (actual time=9.441..9.441 rows=37551 loops=1)
              ->  Seq Scan on customer  (cost=0.00..434.57 rows=37500 width=65) (actual time=1.423..3.800 rows=37551 loops=1)
Planning time: 30.557 ms
  (slice0)    Executor memory: 737K bytes.
  (slice1)    Executor memory: 60666K bytes avg x 4 workers, 60667K bytes max (seg0).
  (slice2)    Executor memory: 78752K bytes avg x 4 workers, 78759K bytes max (seg3).  Work_mem: 17604K bytes max.
  (slice3)    Executor memory: 9736K bytes avg x 4 workers, 9736K bytes max (seg0).  Work_mem: 3465K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 2646.628 ms
```
### Запрос 5: Получение всех деталей, поставляемых конкретным поставщиком, с указанием сведений о поставщике
```sql
EXPLAIN ANALYZE 
SELECT s.s_suppkey
     , s.s_name
     , s.s_address
     , s.s_phone
     , ps.ps_partkey
     , p.p_name
     , p.p_retailprice
     , p.p_comment
  FROM supplier AS s
  JOIN partsupp AS ps ON ps.ps_suppkey = s.s_suppkey 
  JOIN part AS p ON p.p_partkey = ps.ps_partkey
 WHERE s.s_suppkey = 1000;
```
```sql
Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..1336.90 rows=80 width=128) (actual time=12.981..13.972 rows=80 loops=1)
  ->  Hash Join  (cost=0.00..1336.87 rows=20 width=128) (actual time=5.959..13.696 rows=21 loops=1)
        Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
        Extra Text: (seg0)   Hash chain length 1.0 avg, 1 max, using 1 of 131072 buckets.
        ->  Hash Join  (cost=0.00..905.55 rows=20 width=61) (actual time=4.242..11.893 rows=21 loops=1)
              Hash Cond: (part.p_partkey = partsupp.ps_partkey)
              Extra Text: (seg0)   Hash chain length 1.0 avg, 1 max, using 21 of 262144 buckets.
              ->  Seq Scan on part  (cost=0.00..434.96 rows=50000 width=57) (actual time=0.223..3.186 rows=50093 loops=1)
              ->  Hash  (cost=454.85..454.85 rows=20 width=8) (actual time=3.899..3.899 rows=21 loops=1)
                    ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..454.85 rows=20 width=8) (actual time=3.416..3.886 rows=21 loops=1)
                          Hash Key: partsupp.ps_partkey
                          ->  Seq Scan on partsupp  (cost=0.00..454.85 rows=20 width=8) (actual time=0.098..4.313 rows=26 loops=1)
                                Filter: (ps_suppkey = 1000)
        ->  Hash  (cost=431.30..431.30 rows=1 width=71) (actual time=1.659..1.659 rows=1 loops=1)
              ->  Broadcast Motion 4:4  (slice2; segments: 4)  (cost=0.00..431.30 rows=1 width=71) (actual time=1.350..1.657 rows=1 loops=1)
                    ->  Seq Scan on supplier  (cost=0.00..431.30 rows=1 width=71) (actual time=0.079..0.170 rows=1 loops=1)
                          Filter: (s_suppkey = 1000)
Planning time: 9.551 ms
  (slice0)    Executor memory: 1272K bytes.
  (slice1)    Executor memory: 268K bytes avg x 4 workers, 268K bytes max (seg0).
  (slice2)    Executor memory: 496K bytes avg x 4 workers, 496K bytes max (seg0).
  (slice3)    Executor memory: 3664K bytes avg x 4 workers, 3664K bytes max (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 14.575 ms
```

## 2. Построить архитектуру ХД на базе `Data Vault`.
Используем приложенный код к ДЗ для создания `Data Vault` (`DV.sql`).  
Для `link_order_lineitem` и `satellite_lineitem` неправильно формриовался `lineitem_hashkey`, исправлен на
```sql
md5(row(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER) ::text)
```
## 3. Составить запросы из первого пункта и замерить время их выполнения `(\timing)`. Поиграться с планами запросов.
### Запрос 1: Получение заказов клиентов с указанием сведений о заказе и клиент
```sql
EXPLAIN ANALYZE
SELECT hc.customerid
     , sc.customername 
     , sc.customeraddress 
     , sc.customerphone 
     , ho.orderid 
     , so.orderdate 
     , so.shipdate
  FROM hub_customer AS hc
  JOIN link_customer_order AS lco ON lco.customer_hashkey = hc.customer_hashkey 
  JOIN hub_order AS ho ON ho.order_hashkey = lco.order_hashkey  
  JOIN satellite_customer AS sc ON sc.customer_hashkey  = hc.customer_hashkey
  JOIN satellite_order AS so ON so.order_hashkey = ho.order_hashkey;
```
```sql
Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..4386.19 rows=1500000 width=76) (actual time=595.959..1326.278 rows=1500000 loops=1)
  ->  Hash Join  (cost=0.00..4004.86 rows=375000 width=76) (actual time=595.776..1224.744 rows=380107 loops=1)
        Hash Cond: ((link_customer_order.customer_hashkey = hub_customer.customer_hashkey) AND (link_customer_order.customer_hashkey = satellite_customer.customer_hashkey))
        Extra Text: (seg2)   Hash chain length 1.7 avg, 8 max, using 22472 of 32768 buckets.
        ->  Redistribute Motion 4:4  (slice2; segments: 4)  (cost=0.00..2744.06 rows=375000 width=45) (actual time=553.953..952.703 rows=380107 loops=1)
              Hash Key: link_customer_order.customer_hashkey
              ->  Hash Join  (cost=0.00..2691.24 rows=375000 width=45) (actual time=606.052..1253.221 rows=375449 loops=1)
                    Hash Cond: ((hub_order.order_hashkey = satellite_order.order_hashkey) AND (link_customer_order.order_hashkey = satellite_order.order_hashkey))
                    Extra Text: (seg1)   Hash chain length 3.0 avg, 12 max, using 123558 of 131072 buckets.
                    ->  Hash Join  (cost=0.00..1627.47 rows=375000 width=103) (actual time=317.468..683.378 rows=375449 loops=1)
                          Hash Cond: (link_customer_order.order_hashkey = hub_order.order_hashkey)
                          Extra Text: (seg1)   Hash chain length 3.0 avg, 14 max, using 123566 of 131072 buckets.
                          ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..585.03 rows=375000 width=66) (actual time=0.023..118.735 rows=375449 loops=1)
                                Hash Key: link_customer_order.order_hashkey
                                ->  Seq Scan on link_customer_order  (cost=0.00..461.52 rows=375000 width=66) (actual time=0.018..61.929 rows=375705 loops=1)
                          ->  Hash  (cost=448.74..448.74 rows=375000 width=37) (actual time=316.952..316.952 rows=375449 loops=1)
                                ->  Seq Scan on hub_order  (cost=0.00..448.74 rows=375000 width=37) (actual time=0.029..186.574 rows=375449 loops=1)
                    ->  Hash  (cost=449.56..449.56 rows=375000 width=41) (actual time=287.811..287.811 rows=375449 loops=1)
                          ->  Seq Scan on satellite_order  (cost=0.00..449.56 rows=375000 width=41) (actual time=0.019..97.458 rows=375449 loops=1)
        ->  Hash  (cost=936.72..936.72 rows=37500 width=130) (actual time=41.603..41.603 rows=37815 loops=1)
              ->  Hash Join  (cost=0.00..936.72 rows=37500 width=130) (actual time=10.376..29.969 rows=37815 loops=1)
                    Hash Cond: (satellite_customer.customer_hashkey = hub_customer.customer_hashkey)
                    Extra Text: (seg2)   Hash chain length 1.3 avg, 6 max, using 28805 of 65536 buckets.
                    ->  Seq Scan on satellite_customer  (cost=0.00..433.93 rows=37500 width=93) (actual time=0.008..3.548 rows=37815 loops=1)
                    ->  Hash  (cost=432.77..432.77 rows=37500 width=37) (actual time=10.129..10.129 rows=37815 loops=1)
                          ->  Seq Scan on hub_customer  (cost=0.00..432.77 rows=37500 width=37) (actual time=0.011..5.268 rows=37815 loops=1)
Planning time: 44.279 ms
  (slice0)    Executor memory: 279K bytes.
  (slice1)    Executor memory: 60K bytes avg x 4 workers, 60K bytes max (seg0).
  (slice2)    Executor memory: 67768K bytes avg x 4 workers, 67768K bytes max (seg0).  Work_mem: 23466K bytes max.
  (slice3)    Executor memory: 21432K bytes avg x 4 workers, 21432K bytes max (seg0).  Work_mem: 6073K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 1373.439 ms
```
### Запрос 2: Получение подробной информации о заказе с указанием позиций
```sql
EXPLAIN ANALYZE  
SELECT ho.orderid 
     , so.orderdate 
     , so.shipdate 
     , hl.lineitemid 
     , sl.quantity 
     , sl.price 
     , sl.discount 
  FROM hub_order AS ho 
  JOIN link_order_lineitem AS ol ON ol.order_hashkey = ho.order_hashkey 
  JOIN hub_lineitem AS hl ON hl.lineitem_hashkey = ol.lineitem_hashkey 
  JOIN satellite_order AS so ON so.order_hashkey = ho.order_hashkey 
  JOIN satellite_lineitem AS sl ON sl.lineitem_hashkey = hl.lineitem_hashkey;
```
```sql
Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..12256.83 rows=6001215 width=32) (actual time=1345.993..6096.119 rows=6001215 loops=1)
  ->  Hash Join  (cost=0.00..11614.46 rows=1500304 width=32) (actual time=1344.687..5638.448 rows=1502010 loops=1)
        Hash Cond: (hub_lineitem.lineitem_hashkey = satellite_lineitem.lineitem_hashkey)
        Extra Text: (seg0)   Initial batch 0:
(seg0)     Wrote 74820K bytes to inner workfile.
(seg0)     Wrote 66020K bytes to outer workfile.
(seg0)   Initial batches 1..3:
(seg0)     Read 74820K bytes from inner workfile: 24940K avg x 3 nonempty batches, 24984K max.
(seg0)     Read 66020K bytes from outer workfile: 22007K avg x 3 nonempty batches, 22046K max.
(seg0)   Hash chain length 3.0 avg, 13 max, using 494459 of 524288 buckets.Initial batch 0:

        ->  Hash Join  (cost=0.00..7424.74 rows=1500304 width=49) (actual time=345.318..3132.459 rows=1502010 loops=1)
              Hash Cond: (link_order_lineitem.lineitem_hashkey = hub_lineitem.lineitem_hashkey)
              Extra Text: (seg0)   Initial batch 0:
(seg0)     Wrote 57217K bytes to inner workfile.
(seg0)     Wrote 66020K bytes to outer workfile.
(seg0)   Initial batches 1..3:
(seg0)     Read 57217K bytes from inner workfile: 19073K avg x 3 nonempty batches, 19106K max.
(seg0)     Read 66020K bytes from outer workfile: 22007K avg x 3 nonempty batches, 22046K max.
(seg0)   Hash chain length 3.0 avg, 13 max, using 494459 of 524288 buckets.
              ->  Redistribute Motion 4:4  (slice2; segments: 4)  (cost=0.00..3675.24 rows=1500304 width=45) (actual time=0.012..1439.419 rows=1502010 loops=1)
                    Hash Key: link_order_lineitem.lineitem_hashkey
                    ->  Hash Join  (cost=0.00..3463.92 rows=1500304 width=45) (actual time=1136.621..2974.441 rows=1501620 loops=1)
                          Hash Cond: (link_order_lineitem.order_hashkey = hub_order.order_hashkey)
                          Extra Text: (seg1)   Hash chain length 3.0 avg, 14 max, using 123566 of 131072 buckets.
                          ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..1047.23 rows=1500304 width=66) (actual time=0.018..424.360 rows=1501620 loops=1)
                                Hash Key: link_order_lineitem.order_hashkey
                                ->  Seq Scan on link_order_lineitem  (cost=0.00..553.12 rows=1500304 width=66) (actual time=0.011..285.244 rows=1501229 loops=1)
                          ->  Hash  (cost=1438.77..1438.77 rows=375000 width=45) (actual time=1135.247..1135.247 rows=375449 loops=1)
                                ->  Hash Join  (cost=0.00..1438.77 rows=375000 width=45) (actual time=266.027..1002.054 rows=375449 loops=1)
                                      Hash Cond: (satellite_order.order_hashkey = hub_order.order_hashkey)
                                      Extra Text: (seg1)   Hash chain length 3.0 avg, 14 max, using 123566 of 131072 buckets.
                                      ->  Seq Scan on satellite_order  (cost=0.00..449.56 rows=375000 width=41) (actual time=0.024..198.691 rows=375449 loops=1)
                                      ->  Hash  (cost=448.74..448.74 rows=375000 width=37) (actual time=265.409..265.409 rows=375449 loops=1)
                                            ->  Seq Scan on hub_order  (cost=0.00..448.74 rows=375000 width=37) (actual time=0.022..118.782 rows=375449 loops=1)
              ->  Hash  (cost=501.96..501.96 rows=1500304 width=37) (actual time=344.488..344.488 rows=1502010 loops=1)
                    ->  Seq Scan on hub_lineitem  (cost=0.00..501.96 rows=1500304 width=37) (actual time=0.059..118.028 rows=1502010 loops=1)
        ->  Hash  (cost=511.87..511.87 rows=1500304 width=49) (actual time=999.187..999.187 rows=1502010 loops=1)
              ->  Seq Scan on satellite_lineitem  (cost=0.00..511.87 rows=1500304 width=49) (actual time=0.067..479.241 rows=1502010 loops=1)
Planning time: 32.586 ms
  (slice0)    Executor memory: 311K bytes.
  (slice1)    Executor memory: 60K bytes avg x 4 workers, 60K bytes max (seg0).
  (slice2)    Executor memory: 92344K bytes avg x 4 workers, 92344K bytes max (seg0).  Work_mem: 26399K bytes max.
* (slice3)    Executor memory: 92656K bytes avg x 4 workers, 92656K bytes max (seg0).  Work_mem: 29393K bytes max, 117341K bytes wanted.
Memory used:  128000kB
Memory wanted:  470563kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 6255.560 ms
```
### Запрос 3: Получение информации о поставщике и детали для каждого отношения Поставщик-деталь
```sql
EXPLAIN ANALYZE 
SELECT hs.supplierid 
     , ss.suppliername 
     , ss.supplieraddress 
     , ss.supplierphone 
     , hp.partid 
     , sp.partname 
     , sp.partprice 
     , sp.partdescription 
  FROM hub_supplier AS hs
  JOIN link_supplier_part AS lsp ON lsp.supplier_hashkey = hs.supplier_hashkey 
  JOIN hub_part AS hp ON lsp.part_hashkey = hp.part_hashkey 
  JOIN satellite_supplier AS ss ON hs.supplier_hashkey = ss.supplier_hashkey
  JOIN satellite_part AS sp ON hp.part_hashkey = sp.part_hashkey;
```
```sql
Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..3066.94 rows=800000 width=122) (actual time=45.165..531.059 rows=800000 loops=1)
  ->  Hash Join  (cost=0.00..2740.47 rows=200000 width=122) (actual time=45.780..351.877 rows=201276 loops=1)
        Hash Cond: (link_supplier_part.part_hashkey = hub_part.part_hashkey)
        Extra Text: (seg2)   Hash chain length 2.0 avg, 8 max, using 25655 of 32768 buckets.
        ->  Redistribute Motion 4:4  (slice2; segments: 4)  (cost=0.00..1546.31 rows=200000 width=97) (actual time=0.012..178.747 rows=201276 loops=1)
              Hash Key: link_supplier_part.part_hashkey
              ->  Hash Join  (cost=0.00..1485.59 rows=200000 width=97) (actual time=6.504..140.865 rows=200454 loops=1)
                    Hash Cond: (link_supplier_part.supplier_hashkey = hub_supplier.supplier_hashkey)
                    Extra Text: (seg2)   Hash chain length 1.2 avg, 5 max, using 8601 of 32768 buckets.
                    ->  Seq Scan on link_supplier_part  (cost=0.00..447.28 rows=200000 width=66) (actual time=0.013..63.128 rows=200454 loops=1)
                    ->  Hash  (cost=880.04..880.04 rows=10000 width=97) (actual time=5.302..5.302 rows=10000 loops=1)
                          ->  Broadcast Motion 4:4  (slice1; segments: 4)  (cost=0.00..880.04 rows=10000 width=97) (actual time=0.598..4.061 rows=10000 loops=1)
                                ->  Hash Join  (cost=0.00..866.69 rows=2500 width=97) (actual time=1.032..1.996 rows=2516 loops=1)
                                      Hash Cond: (satellite_supplier.supplier_hashkey = hub_supplier.supplier_hashkey)
                                      Extra Text: (seg3)   Hash chain length 1.0 avg, 3 max, using 2467 of 65536 buckets.
                                      ->  Seq Scan on satellite_supplier  (cost=0.00..431.20 rows=2500 width=93) (actual time=0.009..0.354 rows=2516 loops=1)
                                      ->  Hash  (cost=431.12..431.12 rows=2500 width=37) (actual time=0.585..0.585 rows=2516 loops=1)
                                            ->  Seq Scan on hub_supplier  (cost=0.00..431.12 rows=2500 width=37) (actual time=0.014..0.233 rows=2516 loops=1)
        ->  Hash  (cost=953.90..953.90 rows=50000 width=91) (actual time=45.723..45.723 rows=50319 loops=1)
              ->  Hash Join  (cost=0.00..953.90 rows=50000 width=91) (actual time=6.326..38.730 rows=50319 loops=1)
                    Hash Cond: (satellite_part.part_hashkey = hub_part.part_hashkey)
                    Extra Text: (seg2)   Hash chain length 1.4 avg, 6 max, using 35191 of 65536 buckets.
                    ->  Seq Scan on satellite_part  (cost=0.00..434.74 rows=50000 width=87) (actual time=0.006..15.672 rows=50319 loops=1)
                    ->  Hash  (cost=433.37..433.37 rows=50000 width=37) (actual time=6.270..6.270 rows=50319 loops=1)
                          ->  Seq Scan on hub_part  (cost=0.00..433.37 rows=50000 width=37) (actual time=0.007..2.269 rows=50319 loops=1)
Planning time: 29.958 ms
  (slice0)    Executor memory: 279K bytes.
  (slice1)    Executor memory: 848K bytes avg x 4 workers, 848K bytes max (seg0).  Work_mem: 158K bytes max.
  (slice2)    Executor memory: 2392K bytes avg x 4 workers, 2392K bytes max (seg0).  Work_mem: 1265K bytes max.
  (slice3)    Executor memory: 13208K bytes avg x 4 workers, 13208K bytes max (seg0).  Work_mem: 6103K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 553.504 ms
```
### Запрос 4: Получение подробной информации о заказе клиента и позиции
```sql
EXPLAIN ANALYZE
SELECT hc.customerid
     , sc.customername 
     , sc.customeraddress 
     , sc.customerphone 
     , ho.orderid 
     , so.orderdate 
     , so.shipdate
     , hl.lineitemid
     , sl.quantity
     , sl.price
     , sl.discount
  FROM hub_customer AS hc
  JOIN link_customer_order AS lco ON lco.customer_hashkey = hc.customer_hashkey 
  JOIN hub_order AS ho ON ho.order_hashkey = lco.order_hashkey  
  JOIN link_order_lineitem AS lol ON lol.order_hashkey = ho.order_hashkey
  JOIN hub_lineitem AS hl ON lol.lineitem_hashkey = hl.lineitem_hashkey
  JOIN satellite_customer AS sc ON sc.customer_hashkey  = hc.customer_hashkey
  JOIN satellite_order AS so ON so.order_hashkey = ho.order_hashkey
  JOIN satellite_lineitem AS sl ON sl.lineitem_hashkey = hl.lineitem_hashkey;
```
```sql
Gather Motion 4:1  (slice5; segments: 4)  (cost=0.00..18600.49 rows=6001215 width=96) (actual time=1929.093..6790.325 rows=6001215 loops=1)
  ->  Hash Join  (cost=0.00..16673.38 rows=1500304 width=96) (actual time=1806.252..6183.044 rows=1502010 loops=1)
        Hash Cond: (hub_lineitem.lineitem_hashkey = satellite_lineitem.lineitem_hashkey)
        Extra Text: (seg0)   Initial batch 0:
(seg0)     Wrote 87282K bytes to inner workfile.
(seg0)     Wrote 171519K bytes to outer workfile.
(seg0)   Initial batches 1..7:
(seg0)     Read 87282K bytes from inner workfile: 12469K avg x 7 nonempty batches, 12501K max.
(seg0)     Read 171519K bytes from outer workfile: 24503K avg x 7 nonempty batches, 24565K max.
(seg0)   Hash chain length 3.0 avg, 13 max, using 494459 of 524288 buckets.Initial batch 0:

        ->  Hash Join  (cost=0.00..11859.53 rows=1500304 width=113) (actual time=696.429..3452.779 rows=1502010 loops=1)
              Hash Cond: (link_order_lineitem.lineitem_hashkey = hub_lineitem.lineitem_hashkey)
              Extra Text: (seg0)   Initial batch 0:
(seg0)     Wrote 66739K bytes to inner workfile.
(seg0)     Wrote 166186K bytes to outer workfile.
(seg0)   Initial batches 1..7:
(seg0)     Read 66739K bytes from inner workfile: 9535K avg x 7 nonempty batches, 9555K max.
(seg0)     Read 166186K bytes from outer workfile: 23741K avg x 7 nonempty batches, 23793K max.
(seg0)   Hash chain length 1.9 avg, 11 max, using 798396 of 1048576 buckets.Initial batch 0:

              ->  Redistribute Motion 4:4  (slice4; segments: 4)  (cost=0.00..7485.90 rows=1500304 width=109) (actual time=332.245..1942.608 rows=1502010 loops=1)
                    Hash Key: link_order_lineitem.lineitem_hashkey
                    ->  Hash Join  (cost=0.00..6974.04 rows=1500304 width=109) (actual time=1803.956..3372.599 rows=1501620 loops=1)
                          Hash Cond: (link_order_lineitem.order_hashkey = hub_order.order_hashkey)
                          Extra Text: (seg1)   Initial batch 0:
(seg1)     Wrote 35576K bytes to inner workfile.
(seg1)     Wrote 92350K bytes to outer workfile.
(seg1)   Initial batches 1..3:
(seg1)     Read 35576K bytes from inner workfile: 11859K avg x 3 nonempty batches, 11937K max.
(seg1)     Read 92350K bytes from outer workfile: 30784K avg x 3 nonempty batches, 30912K max.
(seg1)   Hash chain length 3.0 avg, 14 max, using 123566 of 131072 buckets.Initial batch 0:

                          ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..1047.23 rows=1500304 width=66) (actual time=0.017..374.727 rows=1501620 loops=1)
                                Hash Key: link_order_lineitem.order_hashkey
                                ->  Seq Scan on link_order_lineitem  (cost=0.00..553.12 rows=1500304 width=66) (actual time=0.004..366.037 rows=1501229 loops=1)
                          ->  Hash  (cost=4068.02..4068.02 rows=375000 width=109) (actual time=1803.860..1803.860 rows=375449 loops=1)
                                ->  Hash Join  (cost=0.00..4068.02 rows=375000 width=109) (actual time=545.150..1652.330 rows=375449 loops=1)
                                      Hash Cond: (hub_order.order_hashkey = satellite_order.order_hashkey)
                                      Extra Text: (seg1)   Initial batch 0:
(seg1)     Wrote 9539K bytes to inner workfile.
(seg1)     Wrote 22283K bytes to outer workfile.
(seg1)   Initial batch 1:
(seg1)     Read 9539K bytes from inner workfile.
(seg1)     Read 22283K bytes from outer workfile.
(seg1)   Hash chain length 3.0 avg, 14 max, using 123566 of 131072 buckets.Initial batch 0:

                                      ->  Hash Join  (cost=0.00..2972.05 rows=375000 width=101) (actual time=306.895..1064.151 rows=375449 loops=1)
                                            Hash Cond: (link_customer_order.order_hashkey = hub_order.order_hashkey)
                                            Extra Text: (seg1)   Initial batch 0:
(seg1)     Wrote 9532K bytes to inner workfile.
(seg1)     Wrote 21560K bytes to outer workfile.
(seg1)   Initial batch 1:
(seg1)     Read 9532K bytes from inner workfile.
(seg1)     Read 21560K bytes from outer workfile.
(seg1)   Hash chain length 1.9 avg, 9 max, using 199305 of 262144 buckets.
                                            ->  Redistribute Motion 4:4  (slice3; segments: 4)  (cost=0.00..1925.15 rows=375000 width=97) (actual time=0.016..456.683 rows=375449 loops=1)
                                                  Hash Key: link_customer_order.order_hashkey
                                                  ->  Hash Join  (cost=0.00..1811.30 rows=375000 width=97) (actual time=49.890..371.892 rows=380107 loops=1)
                                                        Hash Cond: (link_customer_order.customer_hashkey = hub_customer.customer_hashkey)
                                                        Extra Text: (seg2)   Hash chain length 1.7 avg, 7 max, using 22390 of 32768 buckets.
                                                        ->  Redistribute Motion 4:4  (slice2; segments: 4)  (cost=0.00..585.03 rows=375000 width=66) (actual time=0.011..122.692 rows=380107 loops=1)
                                                              Hash Key: link_customer_order.customer_hashkey
                                                              ->  Seq Scan on link_customer_order  (cost=0.00..461.52 rows=375000 width=66) (actual time=0.004..51.733 rows=375705 loops=1)
                                                        ->  Hash  (cost=932.39..932.39 rows=37500 width=97) (actual time=49.824..49.824 rows=37815 loops=1)
                                                              ->  Hash Join  (cost=0.00..932.39 rows=37500 width=97) (actual time=6.757..39.396 rows=37815 loops=1)
                                                                    Hash Cond: (satellite_customer.customer_hashkey = hub_customer.customer_hashkey)
                                                                    Extra Text: (seg2)   Hash chain length 1.3 avg, 6 max, using 28805 of 65536 buckets.
                                                                    ->  Seq Scan on satellite_customer  (cost=0.00..433.93 rows=37500 width=93) (actual time=0.005..3.820 rows=37815 loops=1)
                                                                    ->  Hash  (cost=432.77..432.77 rows=37500 width=37) (actual time=6.474..6.474 rows=37815 loops=1)
                                                                          ->  Seq Scan on hub_customer  (cost=0.00..432.77 rows=37500 width=37) (actual time=0.008..2.041 rows=37815 loops=1)
                                            ->  Hash  (cost=448.74..448.74 rows=375000 width=37) (actual time=306.491..306.491 rows=375449 loops=1)
                                                  ->  Seq Scan on hub_order  (cost=0.00..448.74 rows=375000 width=37) (actual time=0.032..167.458 rows=375449 loops=1)
                                      ->  Hash  (cost=449.56..449.56 rows=375000 width=41) (actual time=238.206..238.206 rows=375449 loops=1)
                                            ->  Seq Scan on satellite_order  (cost=0.00..449.56 rows=375000 width=41) (actual time=0.003..143.201 rows=375449 loops=1)
              ->  Hash  (cost=501.96..501.96 rows=1500304 width=37) (actual time=362.549..362.549 rows=1502010 loops=1)
                    ->  Seq Scan on hub_lineitem  (cost=0.00..501.96 rows=1500304 width=37) (actual time=0.038..120.338 rows=1502010 loops=1)
        ->  Hash  (cost=511.87..511.87 rows=1500304 width=49) (actual time=1109.518..1109.518 rows=1502010 loops=1)
              ->  Seq Scan on satellite_lineitem  (cost=0.00..511.87 rows=1500304 width=49) (actual time=0.125..544.362 rows=1502010 loops=1)
Planning time: 105.687 ms
  (slice0)    Executor memory: 293K bytes.
  (slice1)    Executor memory: 60K bytes avg x 4 workers, 60K bytes max (seg0).
  (slice2)    Executor memory: 60K bytes avg x 4 workers, 60K bytes max (seg0).
  (slice3)    Executor memory: 13240K bytes avg x 4 workers, 13240K bytes max (seg0).  Work_mem: 4785K bytes max.
* (slice4)    Executor memory: 59560K bytes avg x 4 workers, 59560K bytes max (seg0).  Work_mem: 13043K bytes max, 51878K bytes wanted.
* (slice5)    Executor memory: 51633K bytes avg x 4 workers, 51633K bytes max (seg0).  Work_mem: 14707K bytes max, 117341K bytes wanted.
Memory used:  128000kB
Memory wanted:  823385kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 6938.510 ms
```
### Запрос 5: Получение всех деталей, поставляемых конкретным поставщиком, с указанием сведений о поставщике

```sql
EXPLAIN ANALYZE 
SELECT hs.supplierid 
     , ss.suppliername 
     , ss.supplieraddress 
     , ss.supplierphone 
     , hp.partid 
     , sp.partname 
     , sp.partprice 
     , sp.partdescription 
  FROM hub_supplier AS hs
  JOIN link_supplier_part AS lsp ON lsp.supplier_hashkey = hs.supplier_hashkey 
  JOIN hub_part AS hp ON lsp.part_hashkey = hp.part_hashkey 
  JOIN satellite_supplier AS ss ON hs.supplier_hashkey = ss.supplier_hashkey
  JOIN satellite_part AS sp ON hp.part_hashkey = sp.part_hashkey
 WHERE hs.supplierid = 1000;
```
```sql
Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..2278.99 rows=80 width=122) (actual time=92.759..94.261 rows=80 loops=1)
  ->  Hash Join  (cost=0.00..2278.96 rows=20 width=122) (actual time=80.903..92.519 rows=23 loops=1)
        Hash Cond: (satellite_part.part_hashkey = hub_part.part_hashkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 23 of 65536 buckets.
        ->  Seq Scan on satellite_part  (cost=0.00..434.74 rows=50000 width=87) (actual time=0.005..9.087 rows=50319 loops=1)
        ->  Hash  (cost=1824.73..1824.73 rows=20 width=101) (actual time=80.618..80.618 rows=23 loops=1)
              ->  Hash Join  (cost=0.00..1824.73 rows=20 width=101) (actual time=76.338..80.607 rows=23 loops=1)
                    Hash Cond: (hub_part.part_hashkey = link_supplier_part.part_hashkey)
                    Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 23 of 65536 buckets.
                    ->  Seq Scan on hub_part  (cost=0.00..433.37 rows=50000 width=37) (actual time=0.007..2.082 rows=50319 loops=1)
                    ->  Hash  (cost=1378.06..1378.06 rows=20 width=97) (actual time=76.057..76.057 rows=23 loops=1)
                          ->  Redistribute Motion 4:4  (slice2; segments: 4)  (cost=0.00..1378.06 rows=20 width=97) (actual time=65.368..76.041 rows=23 loops=1)
                                Hash Key: link_supplier_part.part_hashkey
                                ->  Hash Join  (cost=0.00..1378.05 rows=20 width=97) (actual time=4.357..67.305 rows=22 loops=1)
                                      Hash Cond: (link_supplier_part.supplier_hashkey = hub_supplier.supplier_hashkey)
                                      Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 65536 buckets.
                                      ->  Seq Scan on link_supplier_part  (cost=0.00..447.28 rows=200000 width=66) (actual time=0.009..37.547 rows=200454 loops=1)
                                      ->  Hash  (cost=863.41..863.41 rows=1 width=97) (actual time=0.645..0.645 rows=1 loops=1)
                                            ->  Broadcast Motion 4:4  (slice1; segments: 4)  (cost=0.00..863.41 rows=1 width=97) (actual time=0.643..0.643 rows=1 loops=1)
                                                  ->  Hash Join  (cost=0.00..863.41 rows=1 width=97) (actual time=0.381..0.694 rows=1 loops=1)
                                                        Hash Cond: (satellite_supplier.supplier_hashkey = hub_supplier.supplier_hashkey)
                                                        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 131072 buckets.
                                                        ->  Seq Scan on satellite_supplier  (cost=0.00..431.20 rows=2500 width=93) (actual time=0.004..0.134 rows=2510 loops=1)
                                                        ->  Hash  (cost=431.20..431.20 rows=1 width=37) (actual time=0.079..0.079 rows=1 loops=1)
                                                              ->  Seq Scan on hub_supplier  (cost=0.00..431.20 rows=1 width=37) (actual time=0.020..0.077 rows=1 loops=1)
                                                                    Filter: (supplierid = 1000)
Planning time: 40.770 ms
  (slice0)    Executor memory: 280K bytes.
  (slice1)    Executor memory: 1108K bytes avg x 4 workers, 1120K bytes max (seg1).  Work_mem: 1K bytes max.
  (slice2)    Executor memory: 592K bytes avg x 4 workers, 592K bytes max (seg0).  Work_mem: 1K bytes max.
  (slice3)    Executor memory: 1192K bytes avg x 4 workers, 1192K bytes max (seg0).  Work_mem: 3K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 94.821 ms
```
## 4. Сравить полученные результаты и описать вывод о том, что бы вы применяли в работе и почему.
Во всех запросах `DataVault` показывает себя хуже `Снежинки`, т.к необходимо сделать много дополнительных `JOIN` прежде чем мы получим результат. Также используется больше памяти при расчёте.  
Как витрины для аналитических запросов такая модель не очень подходит, но можно ее использовать для хранения данных с историчностью. В дальнейшем предрасчитывая единоразово витрины для аналитики, приводя к той же `Снежинке`или `Звезде`.

## 5. Задание со звездочкой `(2 бонусных балла)` - построить модель DWH по якорю. Описать проделанные действия.
Рассмотрим якорную модель на примере `Customer` и `Order`.  
Необходимо реализовать:
1. Якоря — основные сущности в базе данных.
2. Атрибуты — каждая отдельная таблица - это 1 атрибут для якоря
3. Связи — связь между якорями
### 1. Якоря
```sql
-- Якорь для клиента
CREATE TABLE anchor_customer (
    customer_hashkey CHAR(32) PRIMARY KEY,
    customer_id INT NOT NULL UNIQUE,
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL
);

-- Якорь для заказа
CREATE TABLE anchor_order (
    order_hashkey CHAR(32) PRIMARY KEY,
    order_id BIGINT NOT NULL UNIQUE, 
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL
);
```
### 2. Атрибуты
```sql
-- Атрибуты клиента: имя, адрес, телефон
CREATE TABLE attribute_customer_name (
    customer_hashkey CHAR(32) NOT NULL REFERENCES anchor_customer(customer_hashkey),
    customer_name VARCHAR(25) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (customer_href, load_date)
);
CREATE TABLE attribute_customer_address (
    customer_hashkey CHAR(32) NOT NULL REFERENCES anchor_customer(customer_hashkey),
    customer_address VARCHAR(40) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (customer_hashkey, load_date)
);
CREATE TABLE attribute_customer_phone (
    customer_hashkey CHAR(32) NOT NULL REFERENCES anchor_customer(customer_hashkey),
    customer_phone CHAR(15) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (customer_hashkey, load_date)
);

-- Атрибуты заказа: статус, дата заказа, сумма
CREATE TABLE attribute_order_status (
    order_hashkey CHAR(32) NOT NULL REFERENCES anchor_order(order_hashkey),
    order_status CHAR(1) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (order_hashkey, load_date)
);
CREATE TABLE attribute_order_date (
    order_hashkey CHAR(32) NOT NULL REFERENCES anchor_order(order_hashkey),
    order_date DATE NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (order_hashkey, load_date)
);
CREATE TABLE attribute_order_totalprice (
    order_hashkey CHAR(32) NOT NULL REFERENCES anchor_order(order_hashkey),
    totalprice NUMERIC(15, 2) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (order_hashkey, load_date)
);
```
### 3. Связи
```sql
-- Связь клиентов с заказами
CREATE TABLE tie_customer_order (
    customer_hashkey CHAR(32) NOT NULL REFERENCES anchor_customer(customer_hashkey),
    order_hashkey CHAR(32) NOT NULL REFERENCES anchor_order(order_hashkey),
    load_date TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (customer_hashkey, order_hashkey, load_date)
);
```