-- Информация по витриннам данных ——————————————————————————————————————————————
SELECT COUNT(*) FROM customer;  -- 150к  записей, распределение по (c_custkey)
SELECT COUNT(*) FROM orders;    -- 1.5кк записей, распределение по (o_orderkey)
SELECT COUNT(*) FROM lineitem;  -- 6кк   записей, распределение по (l_orderkey, l_linenumber)

-- Запрос 4: Получение подробной информации о заказе клиента и позиции
-- 1. Отбор данных в исходном виде —————————————————————————————————————————————
-- 1.1 Актуализация статистики
VACUUM ANALYZE customer;
VACUUM ANALYZE orders;
VACUUM ANALYZE lineitem;

-- 1.2 Запрос с актуальной статистико
EXPLAIN ANALYZE
SELECT c.c_custkey
     , c.c_name
     , o.o_orderkey
     , o.o_orderdate
     , o.o_orderstatus
     , o.o_totalprice
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment
  FROM customer c 
  JOIN orders o 
    ON c.c_custkey = o.o_custkey
  JOIN lineitem l
    ON o.o_orderkey = l.l_orderkey;
  
Gather Motion 4:1  (slice4; segments: 4)  (cost=0.00..4881.55 rows=5838579 width=84) (actual time=1107.994..5796.423 rows=6001215 loops=1)
  ->  Hash Join  (cost=0.00..3241.02 rows=1459645 width=84) (actual time=1106.492..3470.469 rows=1503360 loops=1)
        Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
        Extra Text: (seg3)   Hash chain length 3.0 avg, 13 max, using 123627 of 131072 buckets.
        ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..942.42 rows=1439697 width=47) (actual time=0.014..734.995 rows=1503360 loops=1)
              Hash Key: lineitem.l_orderkey
              ->  Sequence  (cost=0.00..537.11 rows=1439697 width=47) (actual time=3.788..575.580 rows=1501915 loops=1)
                    ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..537.11 rows=1439697 width=47) (actual time=3.758..475.999 rows=1501915 loops=1)
                          Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
        ->  Hash  (cost=1157.47..1157.47 rows=375000 width=45) (actual time=1106.428..1106.428 rows=375552 loops=1)
              ->  Redistribute Motion 4:4  (slice3; segments: 4)  (cost=0.00..1157.47 rows=375000 width=45) (actual time=17.678..1034.262 rows=375552 loops=1)
                    Hash Key: orders.o_orderkey
                    ->  Hash Join  (cost=0.00..1104.65 rows=375000 width=45) (actual time=21.261..758.192 rows=375274 loops=1)
                          Hash Cond: (orders.o_custkey = customer.c_custkey)
                          Extra Text: (seg1)   Hash chain length 1.1 avg, 4 max, using 34819 of 262144 buckets.
                          ->  Redistribute Motion 4:4  (slice2; segments: 4)  (cost=0.00..514.98 rows=375000 width=26) (actual time=0.054..229.585 rows=375274 loops=1)
                                Hash Key: orders.o_custkey
                                ->  Sequence  (cost=0.00..456.57 rows=375000 width=26) (actual time=1.458..230.003 rows=375552 loops=1)
                                      ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                                            Partitions selected: 87 (out of 87)
                                      ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..456.57 rows=375000 width=26) (actual time=1.433..189.529 rows=375552 loops=1)
                                            Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
                          ->  Hash  (cost=434.57..434.57 rows=37500 width=23) (actual time=16.375..16.375 rows=37551 loops=1)
                                ->  Seq Scan on customer  (cost=0.00..434.57 rows=37500 width=23) (actual time=0.363..4.259 rows=37551 loops=1)
Planning time: 51.564 ms
  (slice0)    Executor memory: 512K bytes.
  (slice1)    Executor memory: 49693K bytes avg x 4 workers, 49693K bytes max (seg0).
  (slice2)    Executor memory: 56168K bytes avg x 4 workers, 56177K bytes max (seg3).
  (slice3)    Executor memory: 6472K bytes avg x 4 workers, 6472K bytes max (seg0).  Work_mem: 1761K bytes max.
  (slice4)    Executor memory: 58488K bytes avg x 4 workers, 58488K bytes max (seg0).  Work_mem: 29340K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 6002.934 ms


/* 
ВЫВОД:
Т.к все витрины распределены по разному ключу, происходит 2 Redistribute Motion
*/


/*
Предалагю следующую оптимизацию:
1. Перераспределение orders по custkey
2. Сделать JOIN customers с витриной из п.1, вставить во временную таблицу перераспределенную по orderkey
3. Перераспределить lineitem по orderkey
4. Сделать JOIN из п.2 с п.3
*/
-- 1
DROP TABLE IF EXISTS orders_rd;

CREATE TABLE orders_rd (LIKE orders INCLUDING ALL)
DISTRIBUTED BY (o_custkey);

INSERT INTO orders_rd 
SELECT * FROM orders;

ANALYZE orders_rd;

-- 2

CREATE TEMPORARY TABLE tmp_cust_ord 
WITH (appendoptimized = true, orientation = column)
AS
SELECT c.c_custkey
     , c.c_name
     , o.o_orderkey
     , o.o_orderdate
     , o.o_orderstatus
     , o.o_totalprice
  FROM customer c 
  JOIN orders o ON c.c_custkey = o.o_custkey
DISTRIBUTED BY (o_orderkey);

ANALYZE tmp_cust_ord;

-- 3
DROP TABLE IF EXISTS lineitem_rd;

CREATE TABLE lineitem_rd (LIKE lineitem INCLUDING ALL)
DISTRIBUTED BY (l_orderkey);

INSERT INTO lineitem_rd 
SELECT * FROM lineitem;

ANALYZE lineitem_rd;
-- 4

SELECT c.c_custkey
     , c.c_name
     , c.o_orderkey
     , c.o_orderdate
     , c.o_orderstatus
     , c.o_totalprice
     , l.l_linenumber
     , l.l_extendedprice
     , l.l_comment
  FROM tmp_cust_ord c
  JOIN lineitem_rd l
    ON c.o_orderkey = l.l_orderkey;

Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..3999.68 rows=6001215 width=84) (actual time=160.656..4655.804 rows=6001215 loops=1)
  ->  Hash Join  (cost=0.00..2313.46 rows=1500304 width=84) (actual time=176.875..1971.470 rows=1503360 loops=1)
        Hash Cond: (lineitem_rd.l_orderkey = tmp_cust_ord.o_orderkey)
        Extra Text: (seg3)   Hash chain length 1.9 avg, 10 max, using 199647 of 262144 buckets.
        ->  Seq Scan on lineitem_rd  (cost=0.00..542.40 rows=1500304 width=47) (actual time=0.969..438.307 rows=1503360 loops=1)
        ->  Hash  (cost=443.17..443.17 rows=375000 width=45) (actual time=175.344..175.344 rows=375552 loops=1)
              ->  Seq Scan on tmp_cust_ord  (cost=0.00..443.17 rows=375000 width=45) (actual time=1.202..42.358 rows=375552 loops=1)
Planning time: 29.430 ms
  (slice0)    Executor memory: 1146K bytes.
  (slice1)    Executor memory: 60686K bytes avg x 4 workers, 60686K bytes max (seg0).  Work_mem: 29340K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 4866.511 ms   


/* 
ВЫВОД:
В результате добавляются дополнительные этапы, но финальный запрос происходит максимально быстро.
При увеличении данных это будет более заметно и ощутимо.  
*/