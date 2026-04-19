-- Информация по витриннам данных ——————————————————————————————————————————————
SELECT COUNT(*) FROM supplier;  -- 10к   записей, распределение по (s_suppkey)
SELECT COUNT(*) FROM part;      -- 200к  записей, распределение по (p_partkey)
SELECT COUNT(*) FROM partsupp;  -- 800к  записей, распределение по (ps_partkey, ps_suppkey)

-- Запрос 5: Получение всех деталей, поставляемых конкретным поставщиком, с указанием сведений о поставщике
-- 1. Отбор данных в исходном виде —————————————————————————————————————————————
-- 1.1 Актуализация статистики
VACUUM ANALYZE supplier;
VACUUM ANALYZE part;
VACUUM ANALYZE partsupp;

-- 1.2 Запрос с актуальной статистикой
EXPLAIN ANALYZE
SELECT *
  FROM supplier s
  JOIN partsupp ps
    ON ps.ps_suppkey = s.s_suppkey 
  JOIN part p
    ON p.p_partkey = ps.ps_partkey
 WHERE s.s_suppkey = 1;

Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..1346.44 rows=187 width=417) (actual time=37.031..44.657 rows=80 loops=1)
  ->  Hash Join  (cost=0.00..1346.18 rows=47 width=417) (actual time=27.590..39.717 rows=27 loops=1)
        Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 65536 buckets.
        ->  Hash Join  (cost=0.00..914.78 rows=47 width=273) (actual time=22.048..34.107 rows=27 loops=1)
              Hash Cond: (part.p_partkey = partsupp.ps_partkey)
              Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 27 of 65536 buckets.
              ->  Seq Scan on part  (cost=0.00..434.96 rows=50000 width=130) (actual time=0.233..6.836 rows=50093 loops=1)
              ->  Hash  (cost=454.88..454.88 rows=47 width=143) (actual time=21.481..21.481 rows=27 loops=1)
                    ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..454.88 rows=47 width=143) (actual time=19.107..20.356 rows=27 loops=1)
                          Hash Key: partsupp.ps_partkey
                          ->  Seq Scan on partsupp  (cost=0.00..454.86 rows=47 width=143) (actual time=0.753..25.430 rows=24 loops=1)
                                Filter: (ps_suppkey = 1)
        ->  Hash  (cost=431.31..431.31 rows=1 width=144) (actual time=3.595..3.595 rows=1 loops=1)
              ->  Broadcast Motion 4:4  (slice2; segments: 4)  (cost=0.00..431.31 rows=1 width=144) (actual time=1.987..3.591 rows=1 loops=1)
                    ->  Seq Scan on supplier  (cost=0.00..431.30 rows=1 width=144) (actual time=0.276..0.564 rows=1 loops=1)
                          Filter: (s_suppkey = 1)
Planning time: 20.006 ms
  (slice0)    Executor memory: 2354K bytes.
  (slice1)    Executor memory: 592K bytes avg x 4 workers, 592K bytes max (seg0).
  (slice2)    Executor memory: 784K bytes avg x 4 workers, 784K bytes max (seg0).
  (slice3)    Executor memory: 2108K bytes avg x 4 workers, 2120K bytes max (seg1).  Work_mem: 5K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 55.959 ms

/* 
ВЫВОД:
Т.к все витрины распределены по разному ключу, происходит Broadcast Motion и Redistribute Motion.
Несмотря на то, что partsupp является связующей витринной и распределена по обоим ключам (ps_partkey, ps_suppkey), это не помогает.
Если бы данных было в разы больше, чем имеется сейчас, я бы сделал следующее:
1. Перераспределил partsupp по ps_suppkey
2. Сделал бы JOIN supplier c таблицей из п.1 (выставив нужные фильтры). Вставил бы во временную таблицу с перераспределением по ps_partkey.
3. Сделал бы JOIN временной таблицы из п.2 c part
*/

-- 3. Витрина с индексом без перераспределения —————————————————————————————————
-- 3.1 Создание витрины с индексом
DROP TABLE IF EXISTS partsupp_idx;
CREATE TABLE partsupp_idx (LIKE partsupp INCLUDING ALL);

DROP INDEX IF EXISTS ps_partkey_idx;
CREATE INDEX ps_partkey_idx ON partsupp_idx(ps_partkey);

DROP INDEX IF EXISTS ps_suppkey_idx;
CREATE INDEX ps_suppkey_idx ON partsupp_idx(ps_suppkey);

INSERT INTO partsupp_idx 
SELECT * FROM partsupp;

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
Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..856.88 rows=2 width=65) (actual time=199.827..201.921 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..856.88 rows=1 width=65) (actual time=199.357..199.578 rows=6 loops=1)
        Hash Cond: (lineitem_idx.l_orderkey = orders.o_orderkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 262144 buckets.
        ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..387.96 rows=1 width=51) (actual time=0.006..0.009 rows=6 loops=1)
              Hash Key: lineitem_idx.l_orderkey
              ->  Bitmap Heap Scan on lineitem_idx  (cost=0.00..387.96 rows=1 width=51) (actual time=26.778..32.419 rows=3 loops=1)
                    Recheck Cond: (l_orderkey = 1)
                    ->  Bitmap Index Scan on l_orderkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.379..0.379 rows=3 loops=1)
                          Index Cond: (l_orderkey = 1)
        ->  Hash  (cost=468.91..468.91 rows=1 width=22) (actual time=199.165..199.165 rows=1 loops=1)
              ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=104.255..199.160 rows=1 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=104.235..199.140 rows=1 loops=1)
                          Filter: (o_orderkey = 1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
Planning time: 31.398 ms
  (slice0)    Executor memory: 168K bytes.
  (slice1)    Executor memory: 1789K bytes avg x 4 workers, 2102K bytes max (seg1).  Work_mem: 25K bytes max.
  (slice2)    Executor memory: 47353K bytes avg x 4 workers, 48357K bytes max (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 213.391 ms

-- План запроса после VACCUM ANALYZE
Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..856.88 rows=2 width=65) (actual time=65.507..80.766 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..856.88 rows=1 width=65) (actual time=65.054..65.253 rows=6 loops=1)
        Hash Cond: (lineitem_idx.l_orderkey = orders.o_orderkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 262144 buckets.
        ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..387.96 rows=1 width=51) (actual time=0.005..0.007 rows=6 loops=1)
              Hash Key: lineitem_idx.l_orderkey
              ->  Bitmap Heap Scan on lineitem_idx  (cost=0.00..387.96 rows=1 width=51) (actual time=1.355..2.090 rows=3 loops=1)
                    Recheck Cond: (l_orderkey = 1)
                    ->  Bitmap Index Scan on l_orderkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.148..0.148 rows=3 loops=1)
                          Index Cond: (l_orderkey = 1)
        ->  Hash  (cost=468.91..468.91 rows=1 width=22) (actual time=64.955..64.955 rows=1 loops=1)
              ->  Sequence  (cost=0.00..468.91 rows=1 width=22) (actual time=27.948..64.951 rows=1 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=1 width=22) (actual time=27.936..64.938 rows=1 loops=1)
                          Filter: (o_orderkey = 1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
Planning time: 25.831 ms
  (slice0)    Executor memory: 168K bytes.
  (slice1)    Executor memory: 1789K bytes avg x 4 workers, 2102K bytes max (seg1).  Work_mem: 25K bytes max.
  (slice2)    Executor memory: 47353K bytes avg x 4 workers, 48357K bytes max (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 82.271 ms

/* 
ВЫВОД:
Добавление индексов для partsupp даже замедлило запрос в текущей цепочке JOIN'ов.
При этом нет Broadcast Motion, что было в 1 запросе из-за чего используется меньше slice'ов
*/