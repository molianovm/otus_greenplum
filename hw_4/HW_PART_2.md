# Параметры хранения для таблиц

1. Распределенный справочник
```sql
CREATE TABLE dict_heap (
    id INT
  , value VARCHAR(255)
  , actual_start_date DATE
  , actual_end_date DATE
) 
DISTRIBUTED REPLICATED;
```
2. Финальная витрина отчёта
```sql
CREATE TABLE final_mart_ao (
    id      INT
  , value   VARCHAR(255)
  , on_date DATE
)
WITH (
    appendoptimized = true
  , orientation     = row
  , compresstype    = ZLIB
  , compresslevel   = 5
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(on_date)
(
    START(DATE '2022-01-01') INCLUSIVE
    END  (DATE '2025-01-01') EXCLUSIVE
    EVERY(INTERVAL '1 month'),
    DEFAULT PARTITION other
);
```
3. Информация по проводкам с ежедневным партицированием 
``` sql
CREATE TABLE transaction_ao (
    transaction_id   BIGINT
  , transaction_date DATE
  , dt_account_key   BIGINT
  , ct_account_key   BIGINT
  , amount           NUMERIC(17,5)
)
WITH (
    appendoptimized = true
  , orientation     = column
  , compresstype    = ZLIB
  , compresslevel   = 5
)
DISTRIBUTED BY (transaction_id)
PARTITION BY RANGE(transaction_date)
(
    START(DATE '2024-01-01') INCLUSIVE
    END  (DATE '2026-01-01') EXCLUSIVE
    EVERY(INTERVAL '1 day'),
    DEFAULT PARTITION other
);
```
4. Информация по счетам, актуальность переносится из года в год.
```sql
CREATE TABLE account_ao (
    account_key       BIGINT
  , account_name      VARCHAR(255)
  , account_number    VARCHAR(20)
  , actual_start_date DATE
  , actual_end_date   DATE
  , open_date         DATE
  , close_date        DATE
)
WITH (
    appendoptimized = true
  , orientation     = column
  , compresstype    = ZLIB
  , compresslevel   = 5
)
DISTRIBUTED BY (account_key)
PARTITION BY RANGE(actual_start_date)
(
    START(DATE '2024-01-01') INCLUSIVE
    END  (DATE '2026-01-01') EXCLUSIVE
    EVERY(INTERVAL '1 year'),
    DEFAULT PARTITION other
);
```