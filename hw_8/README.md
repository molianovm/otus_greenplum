## 1. Создать денормализованную модель данных
Возьмем за основу модели с прошлых занятий, которые объедены все в одну широкую витрину
```sql
CREATE TABLE fact_sales
WITH (appendonly = true, orientation = column) 
AS 
    SELECT c.c_custkey
         , c.c_nationkey
         , r1.r_regionkey AS c_regionkey
         , o.o_orderkey
         , p.p_partkey
         , s.s_suppkey
         , s.s_nationkey
         , n2.n_nationkey
         , r2.r_regionkey AS s_regionkey
         , c.c_name
         , n1.n_name      AS c_nationname
         , r1.r_name      AS c_regionname
         , c.c_address
         , c.c_phone
         , c.c_acctbal
         , c.c_mktsegment
         , o.o_orderstatus
         , o.o_totalprice
         , o.o_orderdate
         , o.o_orderpriority
         , o.o_clerk
         , o.o_shippriority
         , l.l_linenumber
         , l.l_quantity
         , l.l_extendedprice
         , l.l_discount
         , l.l_tax
         , l.l_returnflag
         , l.l_linestatus
         , l.l_shipdate
         , l.l_commitdate
         , l.l_receiptdate
         , l.l_shipinstruct
         , l.l_shipmode
         , p.p_name
         , p.p_mfgr
         , p.p_brand
         , p.p_type
         , p.p_size
         , p.p_container
         , p.p_retailprice
         , s.s_name
         , n2.n_name      AS s_nationname
         , r2.r_name      AS s_regionname
         , s.s_address
         , s.s_phone
         , s.s_acctbal
      FROM customer AS c 
      JOIN nation   AS n1 ON n1.n_nationkey = c.c_nationkey 
      JOIN region   AS r1 ON r1.r_regionkey = n1.n_regionkey 
      JOIN orders   AS o  ON o.o_custkey    = c.c_custkey 
      JOIN lineitem AS l  ON l.l_orderkey   = o.o_orderkey
      JOIN part     AS p  ON p.p_partkey    = l.l_partkey 
      JOIN supplier AS s  ON s.s_suppkey    = l.l_suppkey 
      JOIN nation   AS n2 ON n2.n_nationkey = s.s_nationkey 
      JOIN region   AS r2 ON r2.r_regionkey = n2.n_regionkey 
DISTRIBUTED RANDOMLY;
```
## 2. Настроить материализованные представления для ускорения запросов
Сделаем материализованное представление:
1. Отбирать информацию по заказам за определенный месяц:
```sql
CREATE MATERIALIZED VIEW v_orders 
AS 
    SELECT s.o_orderkey
         , s.o_orderstatus
         , s.o_totalprice
         , s.o_orderdate
         , s.o_shippriority
         , s.l_linenumber
         , s.l_quantity
         , s.l_extendedprice
         , s.l_discount
         , s.l_tax
         , s.l_returnflag
         , s.l_linestatus
         , s.l_commitdate
         , s.l_receiptdate
         , s.l_shipinstruct
         , s.l_shipmode
      FROM fact_sales AS s
     WHERE s.o_orderdate BETWEEN '1996-09-01'::DATE AND '1996-09-30'::DATE
     ORDER BY s.o_orderkey
            , s.l_linenumber
DISTRIBUTED RANDOMLY;
```
2. Отбор информации по поставщикам:
```sql
SELECT DISTINCT
       s.s_suppkey
     , s.s_regionkey
     , s.s_nationkey
     , s.s_name
     , s.s_regionname
     , s.s_nationname
     , s.s_address
     , s.s_phone
     , s.s_acctbal
  FROM fact_sales AS s
```
## 3. Написать запросы с использованием оконных функций
1. Вычисление средней цены заказа за день и сравнение в разрезе каждого заказа
```sql
  WITH wt_data AS (
           SELECT DISTINCT
                  s.o_orderkey
                , s.o_totalprice
                , s.o_orderdate
             FROM fact_sales AS s
       )
SELECT wd.o_orderkey
     , wd.o_totalprice
     , wd.o_orderdate
     , ROUND(
           SUM(wd.o_totalprice) OVER (PARTITION BY wd.o_orderdate) 
         / COUNT(wd.o_orderkey) OVER (PARTITION BY wd.o_orderdate)
         , 2
       ) AS avg_price_on_date
  FROM wt_data AS wd
```
2. Отбор кумлятивной суммы заказов за месяц
```sql
  WITH wt_data AS (
           SELECT DISTINCT
                  s.o_orderkey
                , s.o_totalprice
                , s.o_orderdate
             FROM fact_sales AS s
            WHERE s.o_orderdate BETWEEN '1992-01-01' AND '1992-01-31'
       )
SELECT DISTINCT
       wd.o_orderdate
     , SUM(wd.o_totalprice) OVER (ORDER BY wd.o_orderdate) AS amount
  FROM wt_data AS wd
 ORDER BY wd.o_orderdate
```
3. Сравнение заказов клиентов с их прошлыми заказами
```sql
  WITH wt_data AS (
           SELECT DISTINCT
                  f.c_custkey
                , f.o_orderkey
                , f.o_totalprice
                , f.o_orderdate
            FROM fact_sales AS f  
       )
SELECT wd.*
     , wd.o_totalprice - (LAG(wd.o_totalprice, 1, wd.o_totalprice) OVER (ORDER BY wd.c_custkey, wd.o_orderdate))::NUMERIC AS diff_prev_order_amount
  FROM wt_data AS wd
 ORDER BY wd.c_custkey
         ,wd.o_orderdate 
```
## 4. Проанализировать данные с помощью аналитических функций
1. Информация по региону, количество открытых и закрытых заказов и на какую сумму
```sql
SELECT r.r_name 
     , n.n_name 
     , COUNT(l.l_linestatus)  FILTER (WHERE l.l_linestatus = 'O') AS cnt_o
     , SUM(l.l_extendedprice) FILTER (WHERE l.l_linestatus = 'O') AS amount_o
     , COUNT(l.l_linestatus)  FILTER (WHERE l.l_linestatus = 'F') AS cnt_f
     , SUM(l.l_extendedprice) FILTER (WHERE l.l_linestatus = 'F') AS amount_f
  FROM region   AS r
  JOIN nation   AS n ON n.n_regionkey = r.r_regionkey 
  JOIN supplier AS s ON s.s_nationkey = n.n_nationkey 
  JOIN lineitem AS l ON l.l_suppkey   = s.s_suppkey 
  GROUP BY r.r_name 
         , n.n_name 
```
2. Средний прайс заказов клиентов
```sql
  WITH wt_data AS (
          SELECT DISTINCT
                 s.c_custkey
               , s.o_orderkey
               , s.o_totalprice
            FROM fact_sales AS s
      )
SELECT wd.c_custkey
     , ROUND(AVG(wd.o_totalprice), 2) AS avg_amount 
  FROM wt_data AS wd 
 GROUP BY wd.c_custkey
```
3. В каком приоритете находятся заказы
```sql
SELECT f.o_orderpriority
     , COUNT(f.o_orderpriority)
  FROM fact_sales AS f
 GROUP BY f.o_orderpriority
```