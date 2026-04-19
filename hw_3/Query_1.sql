-- Информация по витриннам данных ——————————————————————————————————————————————
SELECT COUNT(*) FROM customer;  -- 150к  записей, распределение по (c_custkey)
SELECT COUNT(*) FROM orders;    -- 1.5кк записей, распределение по (o_orderkey)

-- Запрос 1: Получение заказов клиентов с указанием сведений о заказе и клиент
-- 1. Отбор данных в исходном виде —————————————————————————————————————————————
-- 1.1 Актуализация статистики
VACUUM ANALYZE customer;
VACUUM ANALYZE orders;

-- 1.2 Запрос с актуальной статистикой
EXPLAIN ANALYZE
SELECT c.c_custkey 
     , c.c_name
     , o.o_orderkey 
     , o.o_orderstatus 
     , o.o_totalprice 
  FROM customer c 
  JOIN orders o 
    ON c.c_custkey = o.o_custkey
 WHERE c.c_custkey = 1;

Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..904.72 rows=16 width=41) (actual time=84.628..90.056 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..904.72 rows=4 width=41) (actual time=50.396..83.589 rows=3 loops=1)
        Hash Cond: (orders.o_custkey = customer.c_custkey)
        Extra Text: (seg3)   Hash chain length 1.0 avg, 1 max, using 1 of 524288 buckets.
        ->  Sequence  (cost=0.00..468.91 rows=4 width=22) (actual time=46.037..78.824 rows=3 loops=1)
              ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4) (never executed)
                    Partitions selected: 87 (out of 87)
              ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..468.91 rows=4 width=22) (actual time=46.024..78.810 rows=3 loops=1)
                    Filter: (o_custkey = 1)
                    Partitions scanned:  Avg 87.0 (out of 87) x 4 workers.  Max 87 parts (seg0).
        ->  Hash  (cost=435.80..435.80 rows=1 width=23) (actual time=0.011..0.011 rows=1 loops=1)
              ->  Broadcast Motion 4:4  (slice1; segments: 4)  (cost=0.00..435.80 rows=1 width=23) (actual time=0.006..0.007 rows=1 loops=1)
                    ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=0.231..2.240 rows=1 loops=1)
                          Filter: (c_custkey = 1)
Planning time: 22.512 ms
  (slice0)    Executor memory: 344K bytes.
  (slice1)    Executor memory: 304K bytes avg x 4 workers, 316K bytes max (seg1).
  (slice2)    Executor memory: 48393K bytes avg x 4 workers, 48401K bytes max (seg3).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 97.968 ms

/* 
ВЫВОД:
Происходит Broadcast Motion из-за чего запрос не является оптимальным
*/


-- 2. Перераспределенная по другому ключу витрина ——————————————————————————————
-- 2.1 Создание перераспределенной таблицы с нужным ключем распределения
DROP TABLE IF EXISTS orders_rd;

CREATE TABLE orders_rd (LIKE orders INCLUDING ALL)
DISTRIBUTED BY (o_custkey);

INSERT INTO orders_rd 
SELECT * FROM orders;

-- 2.2 Запрос без предварительной актуализации статистики
EXPLAIN ANALYZE
SELECT c.c_custkey 
     , c.c_name
     , o.o_orderkey 
     , o.o_orderstatus 
     , o.o_totalprice 
  FROM customer c 
  JOIN orders_rd o 
    ON c.c_custkey = o.o_custkey
 WHERE c.c_custkey = 1;

-- 2.3 Запрос с актуализацией статистики
VACUUM ANALYZE orders_rd;

EXPLAIN ANALYZE
SELECT c.c_custkey 
     , c.c_name
     , o.o_orderkey 
     , o.o_orderstatus 
     , o.o_totalprice 
  FROM customer c 
  JOIN orders_rd o 
    ON c.c_custkey = o.o_custkey
 WHERE c.c_custkey = 1;

-- План запроса до VACCUM ANALYZE
Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..904.92 rows=1 width=41) (actual time=84.106..84.106 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..904.92 rows=1 width=41) (actual time=59.481..83.563 rows=6 loops=1)
        Hash Cond: (customer.c_custkey = orders_rd.o_custkey)
        Extra Text: (seg1)   Hash chain length 6.0 avg, 6 max, using 1 of 524288 buckets.
        ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=0.098..23.617 rows=1 loops=1)
              Filter: (c_custkey = 1)
        ->  Hash  (cost=469.12..469.12 rows=1 width=22) (actual time=58.972..58.972 rows=6 loops=1)
              ->  Seq Scan on orders_rd  (cost=0.00..469.12 rows=1 width=22) (actual time=12.791..58.952 rows=6 loops=1)
                    Filter: (o_custkey = 1)
Planning time: 22.570 ms
  (slice0)    Executor memory: 736K bytes.
  (slice1)    Executor memory: 5004K bytes avg x 4 workers, 5004K bytes max (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 84.943 ms

-- План запроса после VACCUM ANALYZE
Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..904.93 rows=17 width=41) (actual time=60.935..60.936 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..904.93 rows=5 width=41) (actual time=14.842..60.456 rows=6 loops=1)
        Hash Cond: (orders_rd.o_custkey = customer.c_custkey)
        Extra Text: (seg1)   Hash chain length 1.0 avg, 1 max, using 1 of 524288 buckets.
        ->  Seq Scan on orders_rd  (cost=0.00..469.12 rows=5 width=22) (actual time=12.218..57.479 rows=6 loops=1)
              Filter: (o_custkey = 1)
        ->  Hash  (cost=435.80..435.80 rows=1 width=23) (actual time=2.099..2.099 rows=1 loops=1)
              ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=0.186..2.090 rows=1 loops=1)
                    Filter: (c_custkey = 1)
Planning time: 21.089 ms
  (slice0)    Executor memory: 736K bytes.
  (slice1)    Executor memory: 4908K bytes avg x 4 workers, 5004K bytes max (seg1).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 61.662 ms

/* 
ВЫВОД:
Нет лишнего распределение. До сбора статистики Hash chain length > чем после сбора, что замедляет запрос.
*/


-- 3. Витрина с индексом без перераспределения —————————————————————————————————
-- 3.1 Создание витрины с индексом
DROP TABLE IF EXISTS orders_idx;
CREATE TABLE orders_idx (LIKE orders INCLUDING ALL);

DROP INDEX IF EXISTS o_custkey_idx;
CREATE INDEX o_custkey_idx ON orders_idx(o_custkey);

INSERT INTO orders_idx 
SELECT * FROM orders;

-- 3.2 Запрос без предварительной актуализации статистики
EXPLAIN ANALYZE
SELECT c.c_custkey 
     , c.c_name
     , o.o_orderkey 
     , o.o_orderstatus 
     , o.o_totalprice 
  FROM customer c 
  JOIN orders_idx o 
    ON c.c_custkey = o.o_custkey
 WHERE c.c_custkey = 1;


-- 3.3 Запрос с актуализацией статистики
VACUUM ANALYZE orders_idx;

EXPLAIN ANALYZE
SELECT c.c_custkey 
     , c.c_name
     , o.o_orderkey 
     , o.o_orderstatus 
     , o.o_totalprice 
  FROM customer c 
  JOIN orders_idx o 
    ON c.c_custkey = o.o_custkey
 WHERE c.c_custkey = 1;

-- План запроса до VACCUM ANALYZE
Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..823.76 rows=1 width=41) (actual time=7.187..7.188 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..823.76 rows=1 width=41) (actual time=4.694..6.785 rows=6 loops=1)
        Hash Cond: (customer.c_custkey = orders_idx.o_custkey)
        Extra Text: (seg1)   Hash chain length 6.0 avg, 6 max, using 1 of 262144 buckets.
        ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=0.117..2.025 rows=1 loops=1)
              Filter: (c_custkey = 1)
        ->  Hash  (cost=387.96..387.96 rows=1 width=22) (actual time=3.973..3.973 rows=6 loops=1)
              ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..387.96 rows=1 width=22) (actual time=3.471..3.968 rows=6 loops=1)
                    Hash Key: orders_idx.o_custkey
                    ->  Bitmap Heap Scan on orders_idx  (cost=0.00..387.96 rows=1 width=22) (actual time=2.718..3.326 rows=3 loops=1)
                          Recheck Cond: (o_custkey = 1)
                          ->  Bitmap Index Scan on o_custkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.328..0.328 rows=3 loops=1)
                                Index Cond: (o_custkey = 1)
Planning time: 26.738 ms
  (slice0)    Executor memory: 368K bytes.
  (slice1)    Executor memory: 1445K bytes avg x 4 workers, 1750K bytes max (seg0).  Work_mem: 17K bytes max.
  (slice2)    Executor memory: 2344K bytes avg x 4 workers, 2368K bytes max (seg1).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 15.205 ms

-- План запроса после VACCUM ANALYZE
Gather Motion 4:1  (slice2; segments: 4)  (cost=0.00..823.76 rows=1 width=41) (actual time=5.568..5.569 rows=6 loops=1)
  ->  Hash Join  (cost=0.00..823.76 rows=1 width=41) (actual time=2.950..5.241 rows=6 loops=1)
        Hash Cond: (customer.c_custkey = orders_idx.o_custkey)
        Extra Text: (seg1)   Hash chain length 6.0 avg, 6 max, using 1 of 262144 buckets.
        ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=0.077..1.968 rows=1 loops=1)
              Filter: (c_custkey = 1)
        ->  Hash  (cost=387.96..387.96 rows=1 width=22) (actual time=2.724..2.724 rows=6 loops=1)
              ->  Redistribute Motion 4:4  (slice1; segments: 4)  (cost=0.00..387.96 rows=1 width=22) (actual time=2.424..2.719 rows=6 loops=1)
                    Hash Key: orders_idx.o_custkey
                    ->  Bitmap Heap Scan on orders_idx  (cost=0.00..387.96 rows=1 width=22) (actual time=0.984..1.475 rows=3 loops=1)
                          Recheck Cond: (o_custkey = 1)
                          ->  Bitmap Index Scan on o_custkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.144..0.144 rows=3 loops=1)
                                Index Cond: (o_custkey = 1)
Planning time: 29.368 ms
  (slice0)    Executor memory: 368K bytes.
  (slice1)    Executor memory: 1445K bytes avg x 4 workers, 1750K bytes max (seg0).  Work_mem: 17K bytes max.
  (slice2)    Executor memory: 2344K bytes avg x 4 workers, 2368K bytes max (seg1).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 6.386 ms

/* 
ВЫВОД:
Время выполнения с использованием индекса знеачительно ускорилось (при наличии фильтра).
Сбор статистики особо не повлиял на запрос.
Проблемным местом также является перераспределение.
*/


-- 4. Витрина с индексом без перераспределения —————————————————————————————————
-- 4.1 Создание витрины с индексом
DROP TABLE IF EXISTS orders_rd_idx;

CREATE TABLE orders_rd_idx (LIKE orders INCLUDING ALL)
DISTRIBUTED BY (o_custkey);

DROP INDEX IF EXISTS o_custkey_rd_idx;
CREATE INDEX o_custkey_rd_idx ON orders_rd_idx(o_custkey);

INSERT INTO orders_rd_idx 
SELECT * FROM orders;

-- 4.2 Запрос без предварительной актуализации статистики
EXPLAIN ANALYZE
SELECT c.c_custkey 
     , c.c_name
     , o.o_orderkey 
     , o.o_orderstatus 
     , o.o_totalprice 
  FROM customer c 
  JOIN orders_rd_idx o 
    ON c.c_custkey = o.o_custkey
 WHERE c.c_custkey = 1;


-- 4.3 Запрос с актуализацией статистики
VACUUM ANALYZE orders_rd_idx;

EXPLAIN ANALYZE
SELECT c.c_custkey 
     , c.c_name
     , o.o_orderkey 
     , o.o_orderstatus 
     , o.o_totalprice 
  FROM customer c 
  JOIN orders_rd_idx o 
    ON c.c_custkey = o.o_custkey
 WHERE c.c_custkey = 1;

-- План запроса до VACCUM ANALYZE
Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..823.76 rows=1 width=41) (actual time=12.089..12.090 rows=6 loops=1)
  ->  Nested Loop  (cost=0.00..823.76 rows=1 width=41) (actual time=4.451..11.589 rows=6 loops=1)
        Join Filter: true
        ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=3.639..9.837 rows=1 loops=1)
              Filter: (c_custkey = 1)
        ->  Bitmap Heap Scan on orders_rd_idx  (cost=0.00..387.96 rows=1 width=18) (actual time=0.777..1.716 rows=6 loops=1)
              Recheck Cond: ((o_custkey = customer.c_custkey) AND (o_custkey = 1))
              ->  Bitmap Index Scan on o_custkey_rd_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.040..0.040 rows=6 loops=1)
                    Index Cond: ((o_custkey = customer.c_custkey) AND (o_custkey = 1))
Planning time: 22.549 ms
  (slice0)    Executor memory: 368K bytes.
  (slice1)    Executor memory: 722K bytes avg x 4 workers, 1894K bytes max (seg1).  Work_mem: 33K bytes max.
  (slice2)    
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 12.784 ms

-- План запроса после VACCUM ANALYZE
Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..823.76 rows=1 width=41) (actual time=3.495..3.496 rows=6 loops=1)
  ->  Nested Loop  (cost=0.00..823.76 rows=1 width=41) (actual time=0.488..3.216 rows=6 loops=1)
        Join Filter: true
        ->  Seq Scan on customer  (cost=0.00..435.80 rows=1 width=23) (actual time=0.078..1.977 rows=1 loops=1)
              Filter: (c_custkey = 1)
        ->  Bitmap Heap Scan on orders_rd_idx  (cost=0.00..387.96 rows=1 width=18) (actual time=0.408..1.234 rows=6 loops=1)
              Recheck Cond: ((o_custkey = customer.c_custkey) AND (o_custkey = 1))
              ->  Bitmap Index Scan on o_custkey_rd_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.009..0.009 rows=6 loops=1)
                    Index Cond: ((o_custkey = customer.c_custkey) AND (o_custkey = 1))
Planning time: 21.510 ms
  (slice0)    Executor memory: 368K bytes.
  (slice1)    Executor memory: 722K bytes avg x 4 workers, 1894K bytes max (seg1).  Work_mem: 33K bytes max.
  (slice2)    
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 4.023 ms

/* 
ВЫВОД:
Самый быстрый запрос из всех при одинаковых вводных. 
*/