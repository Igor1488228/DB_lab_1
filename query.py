"""Біцан Ігор, КМ-82, лабораторна робота №1
Варіант 8
Порівняти середній бал з Математики у кожному регіоні у 2020 та 2019 роках серед тих кому було зараховано тест"""


import csv
import psycopg2
import psycopg2.errorcodes
import itertools
import datetime


# підключаємося до бази данних та задаємо назви, паролі, хости і тд.
conn = psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='localhost', port="5432")
cursor = conn.cursor()

# якщо вже створили таблицю - видаляємо її
cursor.execute('DROP TABLE IF EXISTS tbl_zno_data;')
conn.commit()


#створюємо таблицю та відкриваємо файл
def create_table():
    with open("Odata2019File.csv", "r", encoding="cp1251") as csv_file:
        header = csv_file.readline()
        header = header.split(';')
        header = [word.strip('"') for word in header]
        header[-1] = header[-1].rstrip('"\n')
        columns_inf = "\n\tYear INT,"
        for word in header:
            if 'Ball' in word:
                columns_inf += '\n\t' + word + ' REAL,'
            elif word == 'Birth':
                columns_inf += '\n\t' + word + ' INT,'
            elif word == "OUTID":
                columns_inf += '\n\t' + word + ' VARCHAR(40) PRIMARY KEY,'
            else:
                columns_inf += '\n\t' + word + ' VARCHAR(255),'

        create_table_query = '''CREATE TABLE IF NOT EXISTS tbl_zno_data (''' + columns_inf.rstrip(',') + '\n);'
        cursor.execute(create_table_query)
        conn.commit()
        return header

header = create_table()


def insert_from_csv(f, year, conn, cursor, times_f):
    """ Функція заповнює таблицю з csv-файлу. Оброблює ситуації, пов'язані з втратою з'єднання з базою. Створює файл, в який записує, час виконання запиту."""

    start_time = datetime.datetime.now()
    times_f.write(str(start_time) + " - відкриття файлу " + f + '\n')
    # відкриваємо файл та починаємо зчитувати дані з csv-файлу
    with open(f, "r", encoding="cp1251") as csv_file:
        print("Считування файлу " + f + ' ...')
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        batches_inserted = 0
        batch_size = 100
        inserted_all = False

        # виконуємо цикл поки не вставили всі рядки
        while not inserted_all:
            try:
                insert_query = '''INSERT INTO tbl_zno_data (year, ''' + ', '.join(header) + ') VALUES '
                count = 0

                for row in csv_reader:
                    count += 1
                    # обробляємо запис
                    for k in row:
                        if row[k] == 'null':
                            pass
                        # текстові значення беремо в лапки
                        elif k.lower() != 'birth' and 'ball' not in k.lower():
                            row[k] = "'" + row[k].replace("'", "''") + "'"
                        # в числових значеннях замінюємо кому на крапку
                        elif 'ball100' in k.lower():
                            row[k] = row[k].replace(',', '.')
                    insert_query += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'

                    # якщо набралося багато рядків
                    if count == batch_size:
                        count = 0
                        insert_query = insert_query.rstrip(',') + ';'
                        cursor.execute(insert_query)
                        conn.commit()
                        batches_inserted += 1
                        insert_query = '''INSERT INTO tbl_zno_data (year, ''' + ', '.join(header) + ') VALUES '

                # якщо досягли кінця файлу
                if count != 0:
                    insert_query = insert_query.rstrip(',') + ';'
                    cursor.execute(insert_query.rstrip(',') + ';')
                    conn.commit()
                inserted_all = True

            except psycopg2.OperationalError as err:
                # якщо з'єднання втрачено
                if err.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                    print("База даних відключилася - чекаємо на відновлення з'єднання...")
                    times_f.write(str(datetime.datetime.now()) + " - втрата з'єднання\n")
                    connection_restored = False
                    while not connection_restored:
                        try:
                            # намагаємось підключитись до бази даних
                            conn = psycopg2.connect(dbname='postgres', user='postgres',
                                                    password='postgres', host='localhost', port="5432")
                            cursor = conn.cursor()
                            times_f.write(str(datetime.datetime.now()) + " - відновлення з'єднання\n")
                            connection_restored = True
                        except psycopg2.OperationalError as err:
                            pass

                    print("З'єднання відновлено!...")
                    csv_file.seek(0, 0)
                    csv_reader = itertools.islice(csv.DictReader(csv_file, delimiter=';'),
                                                  batches_inserted * batch_size, None)

    end_time = datetime.datetime.now()
    times_f.write(str(end_time) + " - файл повністю оброблено\n")
    times_f.write('Витрачено часу на даний файл - ' + str(end_time - start_time) + '\n\n')

    return conn, cursor


times_file = open('times.txt', 'w')
conn, cursor = insert_from_csv("Odata2019File.csv", 2019, conn, cursor, times_file)
conn, cursor = insert_from_csv("Odata2020File.csv", 2020, conn, cursor, times_file)

times_file.close()


def statistical_query():
    """Виконує запит до таблиці та записує результат у csv-файл. """
    select_query = '''
    SELECT regname AS "Область", year AS "Рік", avg(mathBall100) AS "Середній бал"
    FROM tbl_zno_data
    WHERE mathTestStatus = 'Зараховано'
    GROUP BY regname, year
    ORDER BY year, avg(mathBall100) DESC;
    '''
    cursor.execute(select_query)

    with open('result.csv', 'w', encoding="utf-8") as result_csv:
        csv_writer = csv.writer(result_csv)
        csv_writer.writerow(['Область', 'Рік', 'Середній бал з математики'])
        for row in cursor:
            csv_writer.writerow(row)


statistical_query()

cursor.close()
conn.close()
