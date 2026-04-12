-- П.1 ЗАПРОС БЕЗ ФИЛЬТРА ПО ПАРТИЦИЯМ —————————————————————————————————————————
explain analyze
SELECT c.c_custkey
     , c.c_name
     , o.o_orderkey
     , o.o_orderstatus
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_shipdate
     , p.p_name
  FROM customer AS c
  JOIN orders AS o
    ON o.o_custkey = c.c_custkey
  JOIN lineitem AS l
    ON l.l_orderkey  = o.o_orderkey
  JOIN part AS p
    ON p.p_partkey = l.l_partkey
 WHERE c.c_custkey = 143848;
 
-- analyze 
Hash Join  (cost=0.00..1805.52 rows=50000 width=85) (actual time=1458.987..3603.540 rows=98 loops=1)
  Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
  Extra Text: Hash chain length 1.0 avg, 1 max, using 25 of 65536 buckets.
  ->  Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..879.22 rows=1 width=49) (actual time=1280.841..2810.733 rows=6001215 loops=1)
        ->  Hash Join  (cost=0.00..879.22 rows=1 width=49) (actual time=1457.606..2186.506 rows=1503587 loops=1)
              Hash Cond: (part.p_partkey = lineitem.l_partkey)
              Extra Text: (seg0)   Initial batch 0:
(seg0)     Wrote 26440K bytes to inner workfile.
(seg0)     Wrote 1251K bytes to outer workfile.
(seg0)   Overflow batch 1:
(seg0)     Read 26440K bytes from inner workfile.
(seg0)     Read 1251K bytes from outer workfile.
(seg0)   Hash chain length 33.1 avg, 141 max, using 45471 of 262144 buckets.
              ->  Seq Scan on part  (cost=0.00..434.96 rows=50000 width=37) (actual time=0.094..10.666 rows=50093 loops=1)
              ->  Hash  (cost=431.00..431.00 rows=1 width=20) (actual time=1452.003..1452.003 rows=1503587 loops=1)
                    ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..431.00 rows=1 width=20) (actual time=0.021..840.552 rows=1503587 loops=1)
                          Hash Key: lineitem.l_partkey
                          ->  Sequence  (cost=0.00..431.00 rows=1 width=20) (actual time=1.319..508.423 rows=1501915 loops=1)
                                ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                                      Partitions selected: 87 (out of 87)
                                ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..431.00 rows=1 width=20) (actual time=1.303..413.128 rows=1501915 loops=1)
                                      Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
  ->  Hash  (cost=866.80..866.80 rows=1 width=44) (actual time=178.111..178.111 rows=25 loops=1)
        Buckets: 65536  Batches: 1  Memory Usage: 2kB
        ->  Gather Motion 4:1  (slice4; segments: 4)  (cost=0.00..866.80 rows=1 width=44) (actual time=178.103..178.103 rows=25 loops=1)
              ->  Hash Join  (cost=0.00..866.80 rows=1 width=44) (actual time=146.690..177.334 rows=25 loops=1)
                    Hash Cond: (orders.o_custkey = customer.c_custkey)
                    Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 131072 buckets.
                    ->  Redistribute Motion 4:4  (slice3; segments: 4)  (cost=0.00..431.00 rows=1 width=25) (actual time=141.302..171.632 rows=25 loops=1)
                          Hash Key: orders.o_custkey
                          ->  Sequence  (cost=0.00..431.00 rows=1 width=25) (actual time=28.439..162.295 rows=11 loops=1)
                                ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                                      Partitions selected: 87 (out of 87)
                                ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..431.00 rows=1 width=25) (actual time=28.416..162.266 rows=11 loops=1)
                                      Filter: (o_custkey = 143848)
                                      Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
                    ->  Hash  (cost=435.80..435.80 rows=1 width=23) (actual time=4.705..4.705 rows=1 loops=1)
                          ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=4.625..4.700 rows=1 loops=1)
                                Filter: (c_custkey = 143848)
Planning time: 49.506 ms
  (slice0)    Executor memory: 1281K bytes.  Work_mem: 2K bytes max.
  (slice1)    Executor memory: 49615K bytes avg x 4 workers, 49615K bytes max (seg1).
* (slice2)    Executor memory: 75184K bytes avg x 4 workers, 75184K bytes max (seg0).  Work_mem: 42334K bytes max, 70481K bytes wanted.
  (slice3)    Executor memory: 56152K bytes avg x 4 workers, 56161K bytes max (seg3).
  (slice4)    Executor memory: 1344K bytes avg x 4 workers, 1344K bytes max (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Memory wanted:  212941kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 3619.226 ms


-- П.2 ЗАПРОС С ФИЛЬТРОМ ПО ДАТАМ  —————————————————————————————————————————————
explain analyze
SELECT c.c_custkey
     , c.c_name
     , o.o_orderkey
     , o.o_orderstatus
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_shipdate
     , p.p_name
  FROM customer AS c
  JOIN orders AS o
    ON o.o_custkey = c.c_custkey
   AND o.o_orderdate BETWEEN '1994-01-01'::DATE AND '1994-01-31'::DATE
  JOIN lineitem AS l
    ON l.l_orderkey  = o.o_orderkey
   AND l.l_shipdate BETWEEN '1994-01-01'::DATE AND '1994-06-30'::DATE
  JOIN part AS p
    ON p.p_partkey = l.l_partkey
 WHERE c.c_custkey = 143848;
 
 -- analyze 
Hash Join  (cost=0.00..1805.52 rows=50000 width=85) (actual time=111.443..216.197 rows=6 loops=1)
  Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
  Extra Text: Hash chain length 1.0 avg, 1 max, using 1 of 65536 buckets.
  ->  Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..879.22 rows=1 width=49) (actual time=75.037..167.627 rows=451437 loops=1)
        ->  Hash Join  (cost=0.00..879.22 rows=1 width=49) (actual time=84.959..130.842 rows=113008 loops=1)
              Hash Cond: (part.p_partkey = lineitem.l_partkey)
              Extra Text: (seg0)   Hash chain length 3.0 avg, 18 max, using 37868 of 131072 buckets.
              ->  Seq Scan on part  (cost=0.00..434.96 rows=50000 width=37) (actual time=0.082..5.520 rows=50093 loops=1)
              ->  Hash  (cost=431.00..431.00 rows=1 width=20) (actual time=84.301..84.301 rows=113008 loops=1)
                    ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..431.00 rows=1 width=20) (actual time=5.830..60.519 rows=113008 loops=1)
                          Hash Key: lineitem.l_partkey
                          ->  Sequence  (cost=0.00..431.00 rows=1 width=20) (actual time=4.514..39.298 rows=113071 loops=1)
                                ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                                      Partitions selected: 8 (out of 87)
                                ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..431.00 rows=1 width=20) (actual time=4.509..32.046 rows=113071 loops=1)
                                      Filter: ((l_shipdate >= '1994-01-01'::date) AND (l_shipdate <= '1994-06-30'::date))
                                      Partitions scanned:  Avg 8.0 (out of 87) x 4 workers.  Max 8 parts (seg0).
  ->  Hash  (cost=866.80..866.80 rows=1 width=44) (actual time=10.250..10.250 rows=1 loops=1)
        Buckets: 65536  Batches: 1  Memory Usage: 1kB
        ->  Gather Motion 4:1  (slice4; segments: 4)  (cost=0.00..866.80 rows=1 width=44) (actual time=10.247..10.248 rows=1 loops=1)
              ->  Hash Join  (cost=0.00..866.80 rows=1 width=44) (actual time=6.671..9.736 rows=1 loops=1)
                    Hash Cond: (orders.o_custkey = customer.c_custkey)
                    Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 131072 buckets.
                    ->  Redistribute Motion 4:4  (slice3; segments: 4)  (cost=0.00..431.00 rows=1 width=25) (actual time=3.534..6.388 rows=1 loops=1)
                          Hash Key: orders.o_custkey
                          ->  Sequence  (cost=0.00..431.00 rows=1 width=25) (actual time=2.388..2.969 rows=1 loops=1)
                                ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                                      Partitions selected: 3 (out of 87)
                                ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..431.00 rows=1 width=25) (actual time=2.383..2.963 rows=1 loops=1)
                                      Filter: ((o_orderdate >= '1994-01-01'::date) AND (o_orderdate <= '1994-01-31'::date) AND (o_custkey = 143848))
                                      Partitions scanned:  Avg 3.0 (out of 87) x 4 workers.  Max 3 parts (seg0).
                    ->  Hash  (cost=435.80..435.80 rows=1 width=23) (actual time=2.122..2.122 rows=1 loops=1)
                          ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=2.026..2.117 rows=1 loops=1)
                                Filter: (c_custkey = 143848)
Planning time: 59.362 ms
  (slice0)    Executor memory: 1282K bytes.  Work_mem: 1K bytes max.
  (slice1)    Executor memory: 4698K bytes avg x 4 workers, 4698K bytes max (seg0).
  (slice2)    Executor memory: 17768K bytes avg x 4 workers, 17768K bytes max (seg0).  Work_mem: 5298K bytes max.
  (slice3)    Executor memory: 2036K bytes avg x 4 workers, 2037K bytes max (seg2).
  (slice4)    Executor memory: 1344K bytes avg x 4 workers, 1344K bytes max (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 217.046 ms