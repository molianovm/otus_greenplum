### Описание/Пошаговая инструкция выполнения домашнего задания:

#### 1. Настроить ресурсные группы пользователей (администраторы, технические пользователи, бизнес-пользователи)
1. Изначально имеются 2 ресурсные группы `default` и `admin`
```sql
SELECT * FROM gp_toolkit.gp_resgroup_config AS grc;
```
![alt text]({05C2DBBA-0F81-4EB4-A0DF-3E7A3D071312}.png)

2. Создадим еще 2 группы `tech` и `business`:
```sql
-- Группа для технических задач
CREATE RESOURCE GROUP tech_group WITH (
    CONCURRENCY=10
  , CPU_RATE_LIMIT=10
  , MEMORY_LIMIT=20
  , MEMORY_SHARED_QUOTA=80
  , MEMORY_SPILL_RATIO=0
);

-- Группа для бизнес-пользователей
CREATE RESOURCE GROUP business_group WITH (
    CONCURRENCY=30
  , CPU_RATE_LIMIT=30
  , MEMORY_LIMIT=40
  , MEMORY_SHARED_QUOTA=80
  , MEMORY_SPILL_RATIO=0
); 
```
![alt text]({FB4E5700-C64A-4CD9-8BD9-DB3B0F73F266}.png)

#### 2. Создать пользователей на каждую группу. Определить метод аутентификации.
1. Изначально имеется только `user` стандартный `gpadmin`:
```sql
SELECT * FROM pg_user;
```
![alt text]({1E408CE2-18B4-49AD-8EF2-42C3702F7FED}.png)

2. Создадим 3-х пользователей `admin`, `tech` и `business`:
```sql
-- Админ
CREATE ROLE admin_user WITH
    LOGIN
    ENCRYPTED PASSWORD 'AdminPass123!'
    SUPERUSER
    RESOURCE GROUP admin_group;
-- Технический-пользователь
CREATE ROLE tech_user WITH
    LOGIN
    ENCRYPTED PASSWORD 'TechPass456!'
    RESOURCE GROUP tech_group;
-- Бизнес-пользователь
CREATE ROLE business_user WITH
    LOGIN
    ENCRYPTED PASSWORD 'BusinessPass789!'
    RESOURCE GROUP business_group;
```

3. Имеем пользователей:
![alt text]({140A1C1A-626D-44B7-8CC4-25E34449BF2E}.png)
4. Распределение по ресурсным группам:
```sql
SELECT pu.usename 
     , rg.rsgname
  FROM pg_user AS pu
  JOIN pg_roles AS pr
    ON pu.usename = pr.rolname
  JOIN pg_resgroup AS rg
    ON pr.rolresgroup= rg.oid
```
![alt text]({9FF57BDF-9E9D-4F4F-A60A-D486FE6EEDB9}.png)+

#### 3. На отдельной от Greenplum машине проверить доступ до БД через DBeaver/psql. Настроить политики доступа посредством конфигурационных файлов.
1. При попытке подключить по логину и паролю возникает ошибка:
![alt text]({BC277E29-AB22-4249-8CA7-7568CFA9ADF0}.png)
2. На мастере в файле `pg_hba.conf` для всех пользователей прокинут доступ, т.к делается внутри `Docker`, все подключения идут с локальной адреса:
![alt text]({FCAB0910-B30D-4724-BDD3-F7D97E0D8D41}.png)
3. Для того, чтобы изменения вступили в силу, перезагружаю кластер:
![alt text]({D690E27C-0F83-437F-99DC-AD5BDE059FC2}.png)
4. Подключение прошло успешно:
```sql
SELECT pid
     , usename
     , client_addr
     , state
     , query
  FROM pg_stat_activity
 WHERE usename IN ('admin_user', 'tech_user', 'business_user');
```
![alt text]({ED9526C3-E740-40D4-AF17-AAB0DFD663D9}.png)

### Дополнительные задачи по желанию:
#### 4. Настроить шифрование диска с помощью pgcrypt
1. Установим расширение `pgcrypto`
```sql
 CREATE EXTENSION pgcrypto;
 SELECT extname FROM pg_extension;
```
![alt text]({112803B4-E7F1-45BB-8800-D9A5F9EEAED8}.png)

2. Создадим таблицу, вставим 2 записи, одну с использованием шифрование, другую без.
```sql
 CREATE TABLE crypto_test (
     id    BIGINT
   , value VARCHAR
 );
 
 INSERT 
   INTO crypto_test
 VALUES (1, pgp_sym_encrypt('test1', 'key'))
      , (1, 'test1');

SELECT * FROM crypto_test;
```
3. В результате таблица имеет зашифрованную запись и обычную.
![alt text]({92C6F31D-DE0D-486C-9A63-6644528C0106}.png)

P.S Пункты: «Настроить отдельный сервер LDAP и связку с БД» и «Настроить SSL-соединение» оказались непосильными =)