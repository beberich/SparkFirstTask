import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import random
import datetime
import findspark

findspark.init()


def data_quality():
    """Функция для проверки качества данных"""
    pass


def generation_data(*args):
    """Создание кортежа данных"""
    return tuple(*args)


spark = SparkSession.builder.appName("sparkSQL").getOrCreate()
records_cnt = int(input('Введите количество записей?:'))
all_data = []
names = ["Алексей", "Светлана", "Дмитрий", "Виктор", "Иван", "Анна", "Максим", "Ольга", "Анастасия", "Сергей",
         "Даниил", "Василиса", "Григорий", "Марк", "Виктория", "Алина"]
cities = ["Санкт-Петербург", "Новосибирск", "Екатеринбург", "Нижний Новгород", "Челябинск", "Ростов-на-Дону",
          "Красноярск", "Волгоград", "Самара", "Казань", "Сызрань", "Нижневартовск"]
letter_for_email = {'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
                    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'i', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
                    'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h',
                    'ц': 'c', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e',
                    'ю': 'u', 'я': 'ya'}

column_name = ["id", "name", "email", "city", "age", "salary", "registration_date"]

for i in range(1, records_cnt + 1):
    name = names[random.randint(0, len(names) - 1)]
    email = ''.join(letter_for_email[j.lower()] for j in name) + str(
        random.randint(1000, 9999)) + '@' \
            + random.choice(['google', 'yandex', 'bk', 'mail', 'yahoo']) + random.choice(['.ru', '.com', '.bk'])
    city = cities[random.randint(0, len(cities) - 1)]
    age = random.randint(18, 95)
    salary = random.randint(25000, 500_000)
    registration_date = 'registration_date'
    all_data.append(generation_data([i, name, email, city, age, salary, registration_date]))

df = spark.createDataFrame(all_data, column_name)
df.show()

spark.stop()
