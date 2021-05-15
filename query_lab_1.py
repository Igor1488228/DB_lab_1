"""Біцан Ігор, КМ-82, лабораторна робота №1
Варіант 8
Порівняти середній бал з Математики у кожному регіоні у 2020 та 2019 роках серед тих кому було зараховано тест"""

import psycopg2
import csv
import itertools
import time
import datetime


# підключаємося до бази данних та задаємо назви, паролі, хости і тд.
def create_connection(db_name, db_user, db_password, db_host):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port="5432"
        )
        print("З'єднання успішне")
    except psycopg2.DatabaseError as e:
        print(f"{e}")
    return connection

conn = create_connection("postgres", "postgres", "postgres", "localhost")
cursor = conn.cursor()

# якщо вже створили таблицю - видаляємо її
cursor.execute('DROP TABLE IF EXISTS zno_data;')
conn.commit()

#створюємо таблицю та відкриваємо файл
def create_table():

    with open("Odata2019File.csv", "r", encoding="cp1251") as csv_file:
        header = csv_file.readline()
        header = header.split(';')
        header = [elem.strip('"') for elem in header]
        columns = "\n\tYear INT,"
        header[-1] = header[-1].rstrip('"\n')


        for elem in header:

            if 'Ball' in elem:
                columns += '\n\t' + elem + ' REAL,'
            elif elem == 'Birth':
                columns += '\n\t' + elem + ' INT,'
            elif elem == "OUTID":
                columns += '\n\t' + elem + ' VARCHAR(40) PRIMARY KEY,'
            else:
                columns += '\n\t' + elem + ' VARCHAR(255),'

        create_table_query = '''CREATE TABLE IF NOT EXISTS zno_data (''' + columns.rstrip(',') + '\n);'
        cursor.execute(create_table_query)
        conn.commit()
        return header

header = create_table()


def insert_from_csv(f, year, conn, cursor, time_file):
    """ Функція заповнює таблицю з csv-файлу. Оброблює ситуації, пов'язані з втратою з'єднання з базою. Створює файл, в який записує, час виконання запиту."""
    start_time = time.time()
    # відкриваємо файл та починаємо зчитувати дані з csv-файлу
    with open(f, "r", encoding="cp1251") as csv_file:
        print(f + ' ...')
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        batches_inserted = 0
        batch_size = 100
        inserted_all = False

        # виконуємо цикл поки не вставили всі рядки
        while not inserted_all:
            try:
                insert_query = '''INSERT INTO zno_data (year, ''' + ', '.join(header) + ') VALUES '
                count = 0
                for row in csv_reader:
                    count += 1

                    # обробляємо запис
                    for key in row:
                        if row[key] == 'null':
                            pass
                        # текстові значення беремо в лапки
                        elif key.lower() != 'birth' and 'ball' not in key.lower():
                            row[key] = "'" + row[key].replace("'", "''") + "'"
                        # в числових значеннях замінюємо кому на крапку
                        elif 'ball100' in key.lower():
                            row[key] = row[key].replace(',', '.')
                    insert_query += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'

                    # якщо набралося багато рядків
                    if count == batch_size:
                        count = 0
                        insert_query = insert_query.rstrip(',') + ';'
                        cursor.execute(insert_query)
                        conn.commit()
                        batches_inserted += 1
                        insert_query = '''INSERT INTO zno_data (year, ''' + ', '.join(header) + ') VALUES '

                # якщо досягли кінця файлу
                if count != 0:
                    insert_query = insert_query.rstrip(',') + ';'
                    cursor.execute(insert_query)
                    conn.commit()
                inserted_all = True

            except psycopg2.OperationalError as err:
                # якщо з'єднання втрачено
                if err.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                    print("База даних відключилася - чекаємо на відновлення з'єднання...")
                    time_file.write(str(datetime.datetime.now()) + " - втрата з'єднання\n")
                    connection_restored = False
                    while not connection_restored:
                        try:
                            # намагаємось підключитись до бази даних
                            conn = create_connection("postgres", "postgres", "postgres", "localhost")
                            cursor = conn.cursor()
                            time_file.write(str(datetime.datetime.now()) + " - відновлення з'єднання\n")
                            connection_restored = True
                        except psycopg2.OperationalError:
                            pass

                    print("З'єднання відновлено!")
                    csv_file.seek(0, 0)
                    csv_reader = itertools.islice(csv.DictReader(csv_file, delimiter=';'),
                                                  batches_inserted * batch_size, None)

    end_time = time.time() - start_time
    time_file.write(str(end_time) + "сек. - файл " + f + " оброблено\n")

    return conn, cursor


time_file = open('time.txt', 'w')
conn, cursor = insert_from_csv("Odata2019File.csv", 2019, conn, cursor, time_file)
conn, cursor = insert_from_csv("Odata2020File.csv", 2020, conn, cursor, time_file)

time_file.close()


QUERY = '''
SELECT regname AS "Область", year AS "Рік", avg(mathBall100) AS "Середній бал"
FROM zno_data
WHERE mathTestStatus = 'Зараховано'
GROUP BY regname, year
ORDER BY year, avg(mathBall100) DESC;
'''
cursor.execute(QUERY)

# запис результату виконаного завдання у csv файл
with open('result1.csv', 'w', encoding="utf-8") as result_csv:
    csv_writer = csv.writer(result_csv)
    header_row = ['Область', 'Рік', 'Середній бал з математики']
    csv_writer.writerow(header_row)
    for row in cursor:
        csv_writer.writerow(row)

cursor.close()
conn.close()