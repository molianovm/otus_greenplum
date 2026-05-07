# Работа с внешними таблицами

1. Был сгенерирован небольшой CSV (файл test.csv)
2. Закинут в докер контейнер:
![alt text](img/{B4C1EB66-F97A-4B75-B399-A584EA56213B}.png)
3. Создана внешняя таблица с нужной структурой под CSV:

```sql
CREATE EXTERNAL TABLE ext_sales_data (
    sale_id      INT
  , product_name VARCHAR(100)
  , quantity     INT
  , price        NUMERIC(10,2)
  , sale_date    DATE
)
LOCATION ('gpfdist://localhost:8081/test.csv')
FORMAT 'CSV' (DELIMITER ',' HEADER);
```
![alt text]({C54A95B6-C8F7-4B10-A92E-9DEFB80CEA9D}.png)

4. Данные успешно считаны:
```sql
SELECT * FROM ext_sales_data;
```
![alt text](img/{81C8590F-DA97-4AEF-9948-7F8B5772BD9D}.png)