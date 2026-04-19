-- Информация по витриннам данных ——————————————————————————————————————————————
SELECT COUNT(*) FROM supplier;  -- 10к   записей, распределение по (s_suppkey)
SELECT COUNT(*) FROM part;      -- 200к  записей, распределение по (p_partkey)
SELECT COUNT(*) FROM partsupp;  -- 800к  записей, распределение по (ps_partkey, ps_suppkey)

-- Запрос 3: Получение информации о поставщике и детали для каждого отношения Поставщик-деталь
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
    ON p.p_partkey = ps.ps_partkey;

Gather Motion 4:1  (slice3; segments: 4)  (cost=0.00..3385.65 rows=795364 width=417) (actual time=113.960..1644.992 rows=800000 loops=1)
  ->  Hash Join  (cost=0.00..2276.23 rows=198841 width=417) (actual time=112.645..672.658 rows=200372 loops=1)
        Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
        Extra Text: (seg2)   Hash chain length 1.1 avg, 4 max, using 9295 of 65536 buckets.
        ->  Hash Join  (cost=0.00..1432.94 rows=199040 width=273) (actual time=83.826..533.015 rows=200372 loops=1)
              Hash Cond: (partsupp.ps_partkey = part.p_partkey)
              Extra Text: (seg2)   Hash chain length 1.4 avg, 8 max, using 35024 of 65536 buckets.
              ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..590.98 rows=200000 width=143) (actual time=0.017..336.508 rows=200372 loops=1)
                    Hash Key: partsupp.ps_partkey
                    ->  Seq Scan on partsupp  (cost=0.00..448.27 rows=200000 width=143) (actual time=11.080..95.920 rows=200662 loops=1)
              ->  Hash  (cost=434.96..434.96 rows=50000 width=130) (actual time=83.288..83.288 rows=50093 loops=1)
                    ->  Seq Scan on part  (cost=0.00..434.96 rows=50000 width=130) (actual time=14.917..64.285 rows=50093 loops=1)
        ->  Hash  (cost=451.70..451.70 rows=10000 width=144) (actual time=28.690..28.690 rows=10000 loops=1)
              ->  Broadcast Motion 4:4  (slice2; segments: 4)  (cost=0.00..451.70 rows=10000 width=144) (actual time=15.495..24.047 rows=10000 loops=1)
                    ->  Seq Scan on supplier  (cost=0.00..431.22 rows=2500 width=144) (actual time=14.276..14.664 rows=2544 loops=1)
Planning time: 44.010 ms
  (slice0)    Executor memory: 2386K bytes.
  (slice1)    Executor memory: 608K bytes avg x 4 workers, 608K bytes max (seg0).
  (slice2)    Executor memory: 784K bytes avg x 4 workers, 784K bytes max (seg0).
  (slice3)    Executor memory: 22568K bytes avg x 4 workers, 22568K bytes max (seg0).  Work_mem: 8332K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 1690.202 ms

/* 
ВЫВОД:
Т.к данные в supplier мало, произашел Broadcast Motion
Также произашло Redistribute Motion при JOIN part и partsupp
*/



/*
Предалагю следующую оптимизацию:
1. Перераспределение supplier REPLICATED, ожидаем что кратно много данных не будет
2. Перераспределить partsupp по ps_partkey
3. Сделать JOIN всех витрин
*/
-- 1
DROP TABLE IF EXISTS supplier_rd;

CREATE TABLE supplier_rd (LIKE supplier INCLUDING ALL)
DISTRIBUTED REPLICATED;

INSERT INTO supplier_rd 
SELECT * FROM supplier;

ANALYZE supplier_rd;

-- 2
DROP TABLE IF EXISTS partsupp_rd;

CREATE TABLE partsupp_rd (LIKE partsupp INCLUDING ALL)
DISTRIBUTED BY (ps_partkey);

INSERT INTO partsupp_rd 
SELECT * FROM partsupp;

ANALYZE partsupp_rd;

--
EXPLAIN ANALYZE
SELECT
  FROM supplier_rd s
  JOIN partsupp_rd ps
    ON ps.ps_suppkey = s.s_suppkey
  JOIN part p
    ON p.p_partkey = ps.ps_partkey;

Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..1407.09 rows=797210 width=1) (actual time=19.944..457.171 rows=800000 loops=1)
  ->  Hash Join  (cost=0.00..1404.43 rows=199303 width=1) (actual time=20.204..461.010 rows=200372 loops=1)
        Hash Cond: (partsupp_rd.ps_partkey = part.p_partkey)
        Extra Text: (seg2)   Hash chain length 1.1 avg, 4 max, using 45643 of 262144 buckets.
        ->  Hash Join  (cost=0.00..923.73 rows=199303 width=4) (actual time=6.427..83.819 rows=200372 loops=1)
              Hash Cond: (partsupp_rd.ps_suppkey = supplier_rd.s_suppkey)
              Extra Text: (seg2)   Hash chain length 1.0 avg, 3 max, using 9815 of 262144 buckets.
              ->  Seq Scan on partsupp_rd  (cost=0.00..448.38 rows=200000 width=8) (actual time=0.066..21.507 rows=200372 loops=1)
              ->  Hash  (cost=431.87..431.87 rows=10000 width=4) (actual time=2.468..2.468 rows=10000 loops=1)
                    ->  Seq Scan on supplier_rd  (cost=0.00..431.87 rows=10000 width=4) (actual time=0.072..0.798 rows=10000 loops=1)
        ->  Hash  (cost=434.96..434.96 rows=50000 width=4) (actual time=13.538..13.538 rows=50093 loops=1)
              ->  Seq Scan on part  (cost=0.00..434.96 rows=50000 width=4) (actual time=0.095..3.850 rows=50093 loops=1)
Planning time: 41.617 ms
  (slice0)    Executor memory: 1064K bytes.
  (slice1)    Executor memory: 9306K bytes avg x 4 workers, 9306K bytes max (seg0).  Work_mem: 1175K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 481.124 ms

/* 
ВЫВОД:
Больше нет перераспределений, скорость запроса увеличилась кратно.
Данное решение подходит, если не ожидается, что количество поставщиков будет увеличиваться каждый день в разы.
*/