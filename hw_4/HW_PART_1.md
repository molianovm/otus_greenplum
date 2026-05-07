# Работа с транзакциями
Были выполнены различны запросы, часть из них:

1. Попытка обновить 2 разных записи с `READ COMMITTED`:
- При обновлении значений в 1 сессии, 2 видит результат до изменения. При попитке изменить данные во 2 сессии запрос висит. 
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{F6CDDF7F-E557-4206-AB94-3A397B4196AA}.png)
- После коммита в 1 сессии, 2 сессия внесла изменения, уже видит новое значение из 1 сессии, при этом в сессии 1 еще не видны изменения сессии 2, т.к не совершен коммит  
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{AC66B46D-9670-4C60-A62F-2B599D4C9504}.png)

2. Попытка в 1 сессии удалять запись, а во 2 обновить туже запись с `READ COMMITTED`.
- В 1 сессии запись удалена, во 2 видны обе. При попытке обновить происходит зависание
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{DAE985BD-6A07-42FE-9BE1-CCC8687C08B8}.png)
- После коммита в 1 сессии, 2 отработала без ошибок, запись просто исчезла
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{DCFC807D-CB94-499A-9EB3-89D4CE678816}.png)

3. Попытка обновить запись, а в другой сессии удалить таблицу с `READ COMMITTED`
- В 1 сессии изменена запись, пока коммит не совершен 2 сессия не дает удалить таблицу
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{8FD3D39E-9FB6-4F8A-B914-60E17DDAE2E1}.png)
- После коммита в 1 сессии, произошел дроп во 2 сессии. При этом в 1 сессии перестал быть доступен `SELECT`, потому что происходит удаление. 
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{38B7D3A3-7FCB-43AB-98FB-CE31E892A4F9}.png)
- После коммита во 2 сессии ожидаемого произошла ошибка, что таблица не найдена
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{F408343E-7BDF-4906-A130-C017D5857E2F}.png)

4. Попытка обновить 2 разных записи с `SERIALIZABLE`:
- Ситуация отличается от п.1. Несмотря на коммит в 1 сессии, 2 сессия видит только свои изменения. По другим строкам значения такие же, что были на момент старта транзакции. 
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{AC26D7B4-DA78-4B3F-B1F0-C4448595EAC1}.png)

5. Попытка в 1 сессии удалять запись, а во 2 обновить туже запись с `SERIALIZABLE`:
- На этот раз при попытке обновить удлённую запись (п.2), во 2 сессии явно произошла ошибка
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{6D166C6E-377D-45B9-8D6D-A8A1367BF947}.png)

6. Попытка обновить запись, а в другой сессии удалить таблицу с `SERIALIZABLE`:
- Результат получился такой же, как и в п.3 (по крайней мере я не увидел отличий)
![alt text](https://github.com/molianovm/otus_greenplum/blob/main/hw_4/img/{17AEC40C-46AD-4179-8244-C123DF44F03A}.png)