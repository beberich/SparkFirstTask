from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType
import random
from datetime import datetime, timedelta, date
import findspark

findspark.init()


def data_quality(df_for_check):
    """Check data quality"""
    all_records_cnt = df_for_check.count()
    records_cnt_after_filters = df_for_check.filter(
        (length(col('name')) >= 5) & (length(col('city')) >= 7) & (col('age') > 17) & (col('age') < 96) & (
            col('email').contains('@')) & ((col('email').contains('.ru')) | (col('email').contains('.com')))).count()
    if all_records_cnt == records_cnt_after_filters:
        print('Congratulations on data quality is success!')
    else:
        print('The data contains an error. Some records have been deleted!')

    return df_for_check


def generation_data(data_count):
    """Create list of data"""
    all_data = list()
    # Example data
    names = ["Алексей", "Светлана", "Дмитрий", "Виктор", "Иванушка", "Анюта", "Максим", "Ольга", "Анастасия", "Сергей",
             "Даниил", "Василиса", "Григорий", "Маркиз", "Виктория", "Алина"]
    cities = ["Санкт-Петербург", "Новосибирск", "Екатеринбург", "Нижний Новгород", "Челябинск", "Ростов-на-Дону",
              "Красноярск", "Волгоград", "Пятигорск", "Казанкакак", "Сызрань", "Нижневартовск"]
    letter_for_email = {'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
                        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'i', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
                        'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h',
                        'ц': 'c', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e',
                        'ю': 'u', 'я': 'ya'}
    for i in range(1, data_count + 1):
        name = names[random.randint(0, len(names) - 1)]
        email = ''.join(letter_for_email[j.lower()] for j in name) + str(
            random.randint(1000, 9999 + 1)) + '@' \
                + random.choice(['google', 'yandex', 'bk', 'mail', 'yahoo']) + random.choice(['.ru', '.com'])
        city = cities[random.randint(0, len(cities) - 1)]
        age = random.randint(18, 95)
        salary = random.randint(25000, 500_000 + 1)
        registration_date = (datetime.now() - timedelta(days=np.random.randint(1, 366))).strftime('%Y-%m-%d')
        all_data.append((i, name, email, city, age, salary, registration_date))

    return all_data


def create_dataset(all_data):
    """Create dataframe on list of data"""
    global spark  # временное решение
    spark = SparkSession.builder.appName("sparkSQL").getOrCreate()
    column_name = ["id", "name", "email", "city", "age", "salary", "registration_date"]
    test_df = spark.createDataFrame(all_data, column_name)
    test_df = test_df \
        .withColumn('age', col('age').cast(IntegerType())) \
        .withColumn('registration_date', col('registration_date').cast(DateType()))
    return test_df


records_cnt = int(input('Введите количество записей: '))  # пофиксить ввод

# Data manipulation
all_records = generation_data(records_cnt)

df = create_dataset(all_records)

df_after_dq = data_quality(df)

today = datetime.date(datetime.now())
df_pd = df_after_dq.toPandas()
df_pd.set_index('id', inplace=True)
df_pd.to_csv(f'{today.year}-{today.month}-{today.day}-dev.csv')

spark.stop()
