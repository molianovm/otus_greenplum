-- Информация по витриннам данных ——————————————————————————————————————————————
SELECT COUNT(*) FROM orders;    -- 1.5кк записей, распределение по (o_orderkey)
SELECT COUNT(*) FROM lineitem;  -- 6кк   записей, распределение по (l_orderkey, l_linenumber)

-- Запрос 2: Получение подробной информации о заказе с указанием позиций
-- 1. Отбор данных в исходном виде —————————————————————————————————————————————
-- 1.1 Актуализация статистики
VACUUM ANALYZE orders;
VACUUM ANALYZE lineitem;

-- 1.2 Запрос с актуальной статистикой
EXPLAIN ANALYZE
SELECT o.o_orderkey
     , o.o_orderstatus 
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment 
     , l.l_shipdate 
  FROM orders o
  JOIN lineitem l 
    ON o.o_orderkey = l.l_orderkey
 WHERE o.o_orderkey = 1;

Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..1059.85 rows=5 width=65) (actual time=363.573..903.191 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..1059.85 rows=2 width=65) (actual time=120.851..894.770 rows=3 loops=1)
        Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 524288 buckets.
        ->  Sequence  (cost=0.00..590.93 rows=2 width=51) (actual time=22.731..796.247 rows=3 loops=1)
              ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                    Partitions selected: 87 (out of 87)
              ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..590.93 rows=2 width=51) (actual time=22.719..796.232 rows=3 loops=1)
                    Filter: (l_orderkey = 1)
                    Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
        ->  Hash  (cost=468.91..468.91 rows=2 width=22) (actual time=97.786..97.786 rows=1 loops=1)
              ->  Broadcast Motion 4:4  (slice1; segments: 4)  (cost=0.00..468.91 rows=2 width=22) (actual time=91.687..97.782 rows=1 loops=1)
                    ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=36.255..90.677 rows=1 loops=1)
                          ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                                Partitions selected: 87 (out of 87)
                          ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=36.247..90.669 rows=1 loops=1)
                                Filter: (o_orderkey = 1)
                                Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
Planning time: 34.587 ms
  (slice0)    Executor memory: 143K bytes.
  (slice1)    Executor memory: 46329K bytes avg x 4 workers, 46345K bytes max (seg1).
  (slice2)    Executor memory: 64788K bytes avg x 4 workers, 64788K bytes max (seg1).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 904.823 ms

/* 
ВЫВОД:
Используется BroadCast Motion, что очень плохо.
*/

-- 2. Перераспределенная по другому ключу витрина ——————————————————————————————
-- 2.1 Создание перераспределенной таблицы с нужным ключем распределения
DROP TABLE IF EXISTS lineitem_rd;

CREATE TABLE lineitem_rd (LIKE lineitem INCLUDING ALL)
DISTRIBUTED BY (l_orderkey);

INSERT INTO lineitem_rd 
SELECT * FROM lineitem;

-- 2.2 Запрос без предварительной актуализации статистики
EXPLAIN ANALYZE
SELECT o.o_orderkey
     , o.o_orderstatus 
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment 
     , l.l_shipdate 
  FROM orders o
  JOIN lineitem_rd l 
    ON o.o_orderkey = l.l_orderkey
 WHERE o.o_orderkey = 1;

-- 2.3 Запрос с актуализацией статистики
VACUUM ANALYZE lineitem_rd;

EXPLAIN ANALYZE
SELECT o.o_orderkey
     , o.o_orderstatus 
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment 
     , l.l_shipdate 
  FROM orders o
  JOIN lineitem_rd l 
    ON o.o_orderkey = l.l_orderkey
 WHERE o.o_orderkey = 1;

-- План запроса до VACCUM ANALYZE
Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..1060.67 rows=2 width=65) (actual time=899.662..899.663 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..1060.67 rows=1 width=65) (actual time=139.137..899.014 rows=6 loops=1)
        Hash Cond: (lineitem_rd.l_orderkey = orders.o_orderkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 524288 buckets.
        ->  Seq Scan on lineitem_rd  (cost=0.00..591.76 rows=1 width=51) (actual time=52.704..812.166 rows=6 loops=1)
              Filter: (l_orderkey = 1)
        ->  Hash  (cost=468.91..468.91 rows=1 width=22) (actual time=85.509..85.509 rows=1 loops=1)
              ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=29.893..85.504 rows=1 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=29.877..85.487 rows=1 loops=1)
                          Filter: (o_orderkey = 1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
Planning time: 24.170 ms
  (slice0)    Executor memory: 632K bytes.
  (slice1)    Executor memory: 50974K bytes avg x 4 workers, 51097K bytes max (seg1).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 901.261 ms

-- План запроса после VACCUM ANALYZE
Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..1060.67 rows=2 width=65) (actual time=679.180..679.181 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..1060.67 rows=1 width=65) (actual time=129.079..678.720 rows=6 loops=1)
        Hash Cond: (lineitem_rd.l_orderkey = orders.o_orderkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 524288 buckets.
        ->  Seq Scan on lineitem_rd  (cost=0.00..591.76 rows=1 width=51) (actual time=52.077..601.252 rows=6 loops=1)
              Filter: (l_orderkey = 1)
        ->  Hash  (cost=468.91..468.91 rows=1 width=22) (actual time=76.445..76.445 rows=1 loops=1)
              ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=25.207..76.439 rows=1 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=25.180..76.411 rows=1 loops=1)
                          Filter: (o_orderkey = 1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
Planning time: 26.272 ms
  (slice0)    Executor memory: 632K bytes.
  (slice1)    Executor memory: 50974K bytes avg x 4 workers, 51097K bytes max (seg1).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 681.015 ms

/* 
ВЫВОД:
Перераспределение помогло исбавиться от Broadcast Motion. 
Сбор статистики позволил слегка ускорить запрос.
*/

-- 3. Витрина с индексом без перераспределения —————————————————————————————————
-- 3.1 Создание витрины с индексом
DROP TABLE IF EXISTS lineitem_idx;
CREATE TABLE lineitem_idx (LIKE lineitem INCLUDING ALL);

DROP INDEX IF EXISTS l_orderkey_idx;
CREATE INDEX l_orderkey_idx ON lineitem_idx(l_orderkey);

INSERT INTO lineitem_idx 
SELECT * FROM lineitem;

-- 3.2 Запрос без предварительной актуализации статистики
EXPLAIN ANALYZE
SELECT o.o_orderkey
     , o.o_orderstatus 
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment 
     , l.l_shipdate 
  FROM orders o
  JOIN lineitem_idx l 
    ON o.o_orderkey = l.l_orderkey
 WHERE o.o_orderkey = 1;


-- 3.3 Запрос с актуализацией статистики
VACUUM ANALYZE lineitem_idx;

EXPLAIN ANALYZE
SELECT o.o_orderkey
     , o.o_orderstatus 
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment 
     , l.l_shipdate 
  FROM orders o
  JOIN lineitem_idx l 
    ON o.o_orderkey = l.l_orderkey
 WHERE o.o_orderkey = 1;

-- План запроса до VACCUM ANALYZE
Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..856.88 rows=2 width=65) (actual time=90.298..91.343 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..856.88 rows=1 width=65) (actual time=89.453..89.671 rows=6 loops=1)
        Hash Cond: (lineitem_idx.l_orderkey = orders.o_orderkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 262144 buckets.
        ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..387.96 rows=1 width=51) (actual time=0.007..0.009 rows=6 loops=1)
              Hash Key: lineitem_idx.l_orderkey
              ->  Bitmap Heap Scan on lineitem_idx  (cost=0.00..387.96 rows=1 width=51) (actual time=2.609..3.719 rows=3 loops=1)
                    Recheck Cond: (l_orderkey = 1)
                    ->  Bitmap Index Scan on l_orderkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.210..0.210 rows=3 loops=1)
                          Index Cond: (l_orderkey = 1)
        ->  Hash  (cost=468.91..468.91 rows=1 width=22) (actual time=89.238..89.238 rows=1 loops=1)
              ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=30.878..89.233 rows=1 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=30.869..89.223 rows=1 loops=1)
                          Filter: (o_orderkey = 1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
Planning time: 30.972 ms
  (slice0)    Executor memory: 168K bytes.
  (slice1)    Executor memory: 1789K bytes avg x 4 workers, 2102K bytes max (seg1).  Work_mem: 25K bytes max.
  (slice2)    Executor memory: 47353K bytes avg x 4 workers, 48357K bytes max (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 99.940 ms

-- План запроса после VACCUM ANALYZE
Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..856.88 rows=2 width=65) (actual time=77.550..77.935 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..856.88 rows=1 width=65) (actual time=77.112..77.317 rows=6 loops=1)
        Hash Cond: (lineitem_idx.l_orderkey = orders.o_orderkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 262144 buckets.
        ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..387.96 rows=1 width=51) (actual time=0.006..0.007 rows=6 loops=1)
              Hash Key: lineitem_idx.l_orderkey
              ->  Bitmap Heap Scan on lineitem_idx  (cost=0.00..387.96 rows=1 width=51) (actual time=1.445..2.146 rows=3 loops=1)
                    Recheck Cond: (l_orderkey = 1)
                    ->  Bitmap Index Scan on l_orderkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.195..0.195 rows=3 loops=1)
                          Index Cond: (l_orderkey = 1)
        ->  Hash  (cost=468.91..468.91 rows=1 width=22) (actual time=76.968..76.968 rows=1 loops=1)
              ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=25.672..76.965 rows=1 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=25.657..76.950 rows=1 loops=1)
                          Filter: (o_orderkey = 1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
Planning time: 25.718 ms
  (slice0)    Executor memory: 168K bytes.
  (slice1)    Executor memory: 1789K bytes avg x 4 workers, 2102K bytes max (seg1).  Work_mem: 25K bytes max.
  (slice2)    Executor memory: 47353K bytes avg x 4 workers, 48357K bytes max (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 79.530 ms

/* 
ВЫВОД:
Наличие индекса позволило в разы увеличить скорость отбора конкретной записи.
Перераспределение имеется, но затраты на него не большие (при условии наличие фильтра).
*/


-- 4. Витрина с индексом без перераспределения —————————————————————————————————
-- 4.1 Создание витрины с индексом
DROP TABLE IF EXISTS lineitem_rd_idx;

CREATE TABLE lineitem_rd_idx (LIKE lineitem INCLUDING ALL)
DISTRIBUTED BY (l_orderkey);

DROP INDEX IF EXISTS l_orderkey_rd_idx;
CREATE INDEX l_orderkey_rd_idx ON lineitem_rd_idx(l_orderkey);

INSERT INTO lineitem_rd_idx 
SELECT * FROM lineitem;

-- 4.2 Запрос без предварительной актуализации статистики
EXPLAIN ANALYZE
SELECT o.o_orderkey
     , o.o_orderstatus 
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment 
     , l.l_shipdate 
  FROM orders o
  JOIN lineitem_rd_idx l 
    ON o.o_orderkey = l.l_orderkey
 WHERE o.o_orderkey = 1;


-- 4.3 Запрос с актуализацией статистики
VACUUM ANALYZE orders_rd_idx;

EXPLAIN ANALYZE
SELECT o.o_orderkey
     , o.o_orderstatus 
     , o.o_totalprice
     , o.o_orderdate
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment 
     , l.l_shipdate 
  FROM orders o
  JOIN lineitem_rd_idx l 
    ON o.o_orderkey = l.l_orderkey
 WHERE o.o_orderkey = 1;

-- План запроса до VACCUM ANALYZE
Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..856.88 rows=2 width=65) (actual time=84.681..95.706 rows=6 loops=1)
  ->  Nested Loop  (cost=0.00..856.88 rows=1 width=65) (actual time=42.039..84.758 rows=6 loops=1)
        Join Filter: true
        ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=36.238..77.658 rows=1 loops=1)
              ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                    Partitions selected: 87 (out of 87)
              ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=36.230..77.649 rows=1 loops=1)
                    Filter: (o_orderkey = 1)
                    Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
        ->  Bitmap Heap Scan on lineitem_rd_idx  (cost=0.00..387.96 rows=1 width=43) (actual time=5.779..7.073 rows=6 loops=1)
              Recheck Cond: ((l_orderkey = orders.o_orderkey) AND (l_orderkey = 1))
              ->  Bitmap Index Scan on l_orderkey_rd_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=4.760..4.760 rows=6 loops=1)
                    Index Cond: ((l_orderkey = orders.o_orderkey) AND (l_orderkey = 1))
Planning time: 10.440 ms
  (slice0)    Executor memory: 168K bytes.
  (slice1)    Executor memory: 46004K bytes avg x 4 workers, 46995K bytes max (seg1).  Work_mem: 33K bytes max.
  (slice2)    
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 98.332 ms

-- План запроса после VACCUM ANALYZE
Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..856.88 rows=2 width=65) (actual time=84.186..84.187 rows=6 loops=1)
  ->  Nested Loop  (cost=0.00..856.88 rows=1 width=65) (actual time=30.174..83.960 rows=6 loops=1)
        Join Filter: true
        ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=29.403..81.988 rows=1 loops=1)
              ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                    Partitions selected: 87 (out of 87)
              ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=29.395..81.979 rows=1 loops=1)
                    Filter: (o_orderkey = 1)
                    Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
        ->  Bitmap Heap Scan on lineitem_rd_idx  (cost=0.00..387.96 rows=1 width=43) (actual time=0.768..1.967 rows=6 loops=1)
              Recheck Cond: ((l_orderkey = orders.o_orderkey) AND (l_orderkey = 1))
              ->  Bitmap Index Scan on l_orderkey_rd_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.016..0.016 rows=6 loops=1)
                    Index Cond: ((l_orderkey = orders.o_orderkey) AND (l_orderkey = 1))
Planning time: 28.955 ms
  (slice0)    Executor memory: 168K bytes.
  (slice1)    Executor memory: 46004K bytes avg x 4 workers, 46995K bytes max (seg1).  Work_mem: 33K bytes max.
  (slice2)    
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 85.626 ms

/* 
ВЫВОД:
Несмотря на то, что в прошлом запросе было перераспределение, а в этом нет, данный запрос отработал чуть медленнее.
Несмотря на это, данный запрос более оптимальный и универсальный (например, дез фильтра по номеру заказа). 
*/