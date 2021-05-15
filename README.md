# DB_lab_1

Лабораторна робота №1 з бази даних


Даний репозиторій містить наступні файли:

- docker-compose.yaml - для запуску PostgreSQL та pgAdmin через Docker;

- query.py - код Python, який створює таблицю в базі даних, заповнює її, виконує запити та записує результати у csv-файл;

- time.txt - файл з вимірами часу роботи БД;

- result.csv - результат запиту до бази даних.



Інструкція:

Завантажити даний репозиторій, зайти в створену папку.

Додати в цю папку csv-файли з даними ЗНО з математики: "Odata2019File.csv" та "Odata2020File.csv".

У терміналі ввести docker-compose up -d - запуск контейнерів з PostgreSQL та pgAdmin.

Запустити файл query.py на виконання. Для моделювання ситуації, коли база "впала" ввести в терміналі docker-compose down, для відновлення з'єднання - docker-compose up -d.

Щоб знищити результати після завершення роботи, ввести docker-compose down в терміналі.
