### I часть:

#### 1. На уже развернутом кластере Greenplum произведите зеркалирование данных как для вычислительных нод, так и для мастер-ноды.
Изначально запущенный догер не имел зеркал.
Созданы папки для зеркал
```bash
mkdir -p /gpdb/gpdata1/gpseg0_mirror
mkdir -p /gpdb/gpdata2/gpseg1_mirror
mkdir -p /gpdb/gpdata3/gpseg2_mirror
mkdir -p /gpdb/gpdata4/gpseg3_mirror
```
Далее созданы зеркала
```bash
gpaddmirrors -p 1000
```
![alt text]({97437CC4-91F2-49B6-997D-624913EFC774}.png)
Далее создал папку для мастер
```bash
mkdir -p /gpdb/standby_master
```
Выдал права
```bash
chmod 644 /gpdb/gpmaster/gpseg-1/pg_hba.conf
```
Запустил создание зеркала для мастера
```bash
gpinitstandby -s localhost -P 5435 -S /gpdb/standby_master/gpseg-1
```
#### 2. Проверьте статус репликации через системные утилиты.
Теперь зеркала есть у всех
![alt text]({32B69A8A-B1E0-4DD0-A41E-87BA4A3AAC06}.png)
#### 3. Разверните и настройте подключение Grafana и Greenplum через Prometheus.
Добавлены файлы для поднятия докера:
- prometheus.yml
- docker-compose.yaml
Создана общая сеть
```bash
docker network create monitoring
```
Подключен GP к сети
```bash
docker network connect monitoring focused_shirley
```
Был поднят контейнер с Grafana и Prometheus и подключен доступ к GP 
![alt text]({91FC28C2-882D-4E67-AC12-987532D875FE}.png)
### II часть:

#### 1. Произведите симуляцию падения сегментной ноды.
Сначала имеем все сегменты в рабочем состоянии
![alt text]({F4A49901-78CE-41E5-8C6D-611A272663AE}.png)
Увидел, что зеркала «лежат». Сначала поднимем их.
Выдал права, до этого не поднялись из-за отсутствия прав
```bash
chown -R gpadmin:gpadmin /gpdb/gpdata1/gpseg0_mirror
chown -R gpadmin:gpadmin /gpdb/gpdata2/gpseg1_mirror
chown -R gpadmin:gpadmin /gpdb/gpdata3/gpseg2_mirror
chown -R gpadmin:gpadmin /gpdb/gpdata4/gpseg3_mirror
```
Поднимаю зеркала
```bash
gprecoverseg -a -F
```
Теперь все в порядке
![alt text]({6AF38FF8-E3E6-4046-8946-EED9898810C6}.png)
Смоделируем падение сегмента «0»
![alt text]({44BF35F2-946B-48F8-B039-F0AE1F2CDA0A}.png)
```bash
kill -9 $(ps aux | grep 6000 | grep postgres | grep -v grep | awk '{print $2}')
```
#### 2. Убедитесь, что БД не упала, и mirror-сегменты включены.
![alt text]({6F38C784-AA30-41B0-8868-DFB4B8B0C8FF}.png)
Через какое-то время 7000 порт стал работать вместо 6000
БД не упала, запросы выполняются
![alt text]({9ECCFC40-BAA3-4119-B05F-DE1B17E33A29}.png)

#### 3. Устраните проблему, поднимите primary-сегменты и произведите ребалансировку данных.
Запустил для поднятия сегмента
```bash
gprecoverseg -F
```
Сегмент снова работает yf 6000 порту
![alt text]({BFE21B28-E858-420E-8874-8F33E11B04BC}.png)
Запустил ребалансировку
```bash
gprecoverseg -r
```
![alt text]({F7C27E93-F476-4820-8DF6-9B410472715F}.png)