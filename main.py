from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr
from pyspark.sql.functions import sum as pyspark_sum
import pyspark.sql.functions as F
import matplotlib.pyplot as plt


def data_preparation():
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv('games.csv', header=True)

    print('\n\033[92m' + 'Подготовка данных' + '\033[0m\n')

    # Преобразуем названия столбцов в lowercase
    data = data.toDF(*[col_name.lower() for col_name in data.columns])

    print('\033[91m' + 'Названия столбцов для проверки преобразования в нижний регистр:'
          + '\033[0m' + f'\n{data.columns}\n')

    # Заменяем тип данных на int в столбце year_of_release, заменяя nan на 0, так как год выпуска является целочисленным
    # значением
    data = data.withColumn('year_of_release',
                           data['year_of_release'].cast('integer')).fillna(0, subset=['year_of_release'])
    print('\033[91m' + 'Уникальные значения в столбце' + '\033[94m' + ' "year_of_release" ' + '\033[91m'
          + 'для проверки преобразования типов:\n' + '\033[0m' +
          f'{data.select("year_of_release").distinct().rdd.flatMap(lambda x: x).collect()}\n')

    # Заменяем 'tbd' значения в столбце user_score для корректного преобразования и последующей обработки
    data = data.withColumn('user_score', col('user_score').cast('float')).fillna(0.0, subset=['user_score'])
    print('\033[91m' + 'Уникальные значения в столбце' + '\033[94m' + ' "user_score" ' + '\033[91m' +
          'для проверки преобразования типов:\n' + '\033[0m' +
          f'{data.select("user_score").distinct().rdd.flatMap(lambda x: x).collect()}\n')

    # Заменяем nan значения в столбце critic_score и преобразуем его в целочисленный тип, так как оценка критиков
    # является целочисленным значением от 1 до 100
    data = data.withColumn('critic_score', data['critic_score'].cast('integer')).fillna(0, subset=['critic_score'])
    print('\033[91m' + 'Уникальные значения в столбце' + '\033[94m' + ' "critic_score" ' + '\033[91m'
          + 'для проверки преобразования типов:\n' + '\033[0m' +
          f'{data.select("critic_score").distinct().rdd.flatMap(lambda x: x).collect()}\n')

    # Преобразуем пустые значения в столбце, который расшифровывается как "Rating Pending"
    data = data.fillna('RP', subset=['rating'])

    # Считаем сумму всех продаж и записываем ее в отдельный столбец
    data = data.withColumn('sum_sales', expr('na_sales + eu_sales + jp_sales + other_sales'))

    # Удаляем записи, в которых есть nan значения в столбцах name и genre (в нашем случае их две)
    data = data.dropna(subset=['name', 'genre'])

    # Подводим итоги подготовки файла к работе
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "name": ' +
          '\033[0m' + f'{data.filter(col("name").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "platform": ' +
          '\033[0m' + f'{data.filter(col("platform").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "year_of_release": ' +
          '\033[0m' + f'{data.filter(col("year_of_release").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "genre": ' +
          '\033[0m' + f'{data.filter(col("genre").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "na_sales": ' +
          '\033[0m' + f'{data.filter(col("na_sales").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "eu_sales": ' +
          '\033[0m' + f'{data.filter(col("eu_sales").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "jp_sales": ' +
          '\033[0m' + f'{data.filter(col("jp_sales").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "other_sales": ' +
          '\033[0m' + f'{data.filter(col("other_sales").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "critic_score": ' +
          '\033[0m' + f'{data.filter(col("critic_score").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "user_score": ' +
          '\033[0m' + f'{data.filter(col("user_score").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "rating": ' +
          '\033[0m' + f'{data.filter(col("rating").isNull()).count()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "sum_sales": ' +
          '\033[0m' + f'{data.filter(col("sum_sales").isNull()).count()}\n')

    # Возвращаем преобразованные данные для дальнейшей их обработки
    return data


def exploratory_analysis(data):
    spark_df = data

    print('\033[92m' + 'Исследовательский анализ данных' + '\033[0m\n')

    print('\033[91m' + 'Кол-во игр выпущенных в период' + '\033[94m' + ' с 1980 по 1990: ' + '\033[0m' +
          f'{spark_df.filter((spark_df["year_of_release"] >= 1980) & (spark_df["year_of_release"] <= 1990)).count()}')
    print('\033[91m' + 'Кол-во игр выпущенных в период' + '\033[94m' + ' с 1990 по 2000: ' + '\033[0m' +
          f'{spark_df.filter((spark_df["year_of_release"] >= 1990) & (spark_df["year_of_release"] <= 2000)).count()}')
    print('\033[91m' + 'Кол-во игр выпущенных в период' + '\033[94m' + ' с 2000 по 2010: ' + '\033[0m' +
          f'{spark_df.filter((spark_df["year_of_release"] >= 2000) & (spark_df["year_of_release"] <= 2010)).count()}')
    print('\033[91m' + 'Кол-во игр выпущенных в период' + '\033[94m' + ' с 2010 по 2020: ' + '\033[0m' +
          f'{spark_df.filter((spark_df["year_of_release"] >= 2010) & (spark_df["year_of_release"] <= 2020)).count()}\n')

    # Составляем список платформ по продажам и сортируем его по убыванию
    platform_sales = spark_df.groupby('platform').agg(F.sum('sum_sales').alias('total_sales')).sort(F.desc('total_sales'))
    # Выбираем верхние 5 платформ
    top_platforms = platform_sales.limit(5).select('platform').rdd.flatMap(lambda x: x).collect()
    print('\033[91m' + 'Топ 5 платформ по продажам игр: ' + '\033[0m' + f'{top_platforms}\n'
          + '\033[94m' + 'Более подробные данные можно увидеть на всплывающем графике' + '\033[0m\n')

    # Делаем выборку из исходных данных только по выбранным 5 платформам
    filtered_data = spark_df.filter(spark_df['platform'].isin(top_platforms))
    # Строим распределение по годам для каждой платформы
    platform_sales_by_year = filtered_data.groupby('platform').pivot('year_of_release').agg(F.sum('sum_sales')).na.fill(0)

    # Выводим полученные данные
    platform_sales_by_year.show()

    # Выполняем прогноз по продажам на 2017 год на основе данных за актуальный период 2010-2016 год
    actual_period_data = spark_df.filter((spark_df["year_of_release"] >= 2010) & (spark_df["year_of_release"] <= 2016))
    sales_data = actual_period_data.groupby('year_of_release').agg(F.sum('sum_sales').alias('total_sales'))

    # Производим вычисление разницы в продажах между годами и делаем прогноз на 2017 год
    sales_diff = sales_data.withColumn('sales_diff', F.col('total_sales') - F.lag('total_sales').over(Window.orderBy('year_of_release')))
    sales_diff = sales_diff.filter(sales_diff['year_of_release'] >= 2011)
    sales_array = sales_diff.select('sales_diff').rdd.flatMap(lambda x: x).collect()
    forecast_2017 = sales_data.filter(sales_data['year_of_release'] == 2016).select('total_sales').rdd.flatMap(lambda x: x).collect()[0] + sum(sales_array) / len(sales_array)
    print('\033[91m' + 'Прогнозируемые продажи на 2017 год на основе предыдущих годов: ' + '\033[0m' +
          f'{forecast_2017}\n')

    # Составляем топ платформ по продажам
    platform_sales = spark_df.groupby('platform').agg(F.sum('sum_sales').alias('total_sales')).sort(F.desc('total_sales'))
    print('\033[91m' + 'Продажи по топ-5 платформам:' + '\033[0m')
    top_platform_sales = platform_sales.limit(5).toPandas()
    for index, row in top_platform_sales.iterrows():
        platform = row['platform']
        actual_data_first = spark_df.filter((spark_df['year_of_release'] == 2016) & (spark_df['platform'] == platform)).agg(F.sum('sum_sales').alias('total_sales')).collect()[0]['total_sales']
        actual_data_second = spark_df.filter((spark_df['year_of_release'] == 2015) & (spark_df['platform'] == platform)).agg(F.sum('sum_sales').alias('total_sales')).collect()[0]['total_sales']
        if actual_data_first and actual_data_second:
            if actual_data_first > actual_data_second:
                parameter = 'Возрастает'
            else:
                parameter = 'Убывает'
        else:
            parameter = 'Продажи отсутствуют'
        print(f'{platform} - {row["total_sales"]} - {parameter}')

    # График с усами по продажам на определенные платформы
    platform_sales = spark_df.groupby('platform').agg(F.sum('sum_sales').alias('total_sales')).sort(F.desc('total_sales'))
    top_platforms = [row['platform'] for row in platform_sales.limit(5).collect()]
    sales_data = spark_df.filter(spark_df['platform'].isin(top_platforms)).select('platform', 'sum_sales')
    sales_data = sales_data.withColumn('platform', F.when(sales_data['platform'].isin(top_platforms), sales_data['platform']).otherwise('Other'))
    sales_data.toPandas().boxplot(column='sum_sales', by='platform')
    plt.xlabel('Платформа')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение глобальных продаж игр по платформам')
    plt.show()
    plt.close()

    # Влияние отзывов критиков и пользователей на продажи игр
    platform_data = spark_df.filter(spark_df['platform'] == 'PS2').na.drop(subset=['user_score', 'critic_score'])
    plt.scatter(platform_data.select('user_score').rdd.flatMap(lambda x: [10 * i for i in x]).collect(),
                platform_data.select('sum_sales').rdd.flatMap(lambda x: x).collect(),
                label='Отзывы пользователей')
    plt.scatter(platform_data.select('critic_score').rdd.flatMap(lambda x: x).collect(),
                platform_data.select('sum_sales').rdd.flatMap(lambda x: x).collect(),
                label='Отзывы критиков')
    plt.xlabel('Отзывы')
    plt.ylabel('Глобальные продажи')
    plt.title('Влияние отзывов на продажи - платформа PS2')
    plt.legend()
    plt.show()
    plt.close()

    # Вывод матрицы корреляции
    correlation = platform_data.select('user_score', 'critic_score', 'sum_sales').toPandas().corr()
    print('\n\033[91m' + 'Матрица корреляции по продажам и оценкам:' + '\033[0m')
    print(correlation)

    # Группируем данные по жанру и суммируем продажи
    genre_sales = spark_df.groupby('genre').agg(F.sum('sum_sales').alias('total_sales')).sort(F.desc('total_sales'))

    # Строим график столбцов для распределения продаж по жанрам
    genre_sales_pd = genre_sales.toPandas()
    genre_sales_pd.plot(kind='bar', x='genre', y='total_sales', color='blue')
    plt.xlabel('Жанр')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение продаж по жанрам')
    plt.xticks(rotation=45)
    plt.show()
    plt.close()

    # Выделяем самые прибыльные жанры
    top_genres = genre_sales_pd.head(5)
    print('\n\033[91m' + 'Самые прибыльные жанры:' + '\033[0m')
    print(top_genres.head())

    print('\n\033[91m' + 'Жанры с высокими продажами:' + '\033[0m')
    print(genre_sales_pd.head())

    print('\n\033[91m' + 'Жанры с низкими продажами:' + '\033[0m')
    print(genre_sales_pd.tail())


def user_potret(data):
    print('\n\033[92m' + 'Портрет пользователя в регионах:' + '\033[0m')

    # Самые популярные платформы в регионах
    platform_sales = data.groupby('platform').agg(pyspark_sum('na_sales').alias('na_sales'),
                                                  pyspark_sum('eu_sales').alias('eu_sales'),
                                                  pyspark_sum('jp_sales').alias('jp_sales'))
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' платформы ' + '\033[91m' + 'в регионе' + '\033[94m' + ' NA:' + '\033[0m')
    print(platform_sales.orderBy('na_sales', ascending=False).limit(5).toPandas())
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' платформы ' + '\033[91m' + 'в регионе' + '\033[94m' + ' EU:' + '\033[0m')
    print(platform_sales.orderBy('eu_sales', ascending=False).limit(5).toPandas())
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' платформы ' + '\033[91m' + 'в регионе' + '\033[94m' + ' JP:' + '\033[0m')
    print(platform_sales.orderBy('jp_sales', ascending=False).limit(5).toPandas())

    # Самые популярные жанры в регионах
    genre_sales = data.groupby('genre').agg(pyspark_sum('na_sales').alias('na_sales'),
                                            pyspark_sum('eu_sales').alias('eu_sales'),
                                            pyspark_sum('jp_sales').alias('jp_sales'))
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' жанры ' + '\033[91m' + 'в регионе' + '\033[94m' + ' NA:' + '\033[0m')
    print(genre_sales.orderBy('na_sales', ascending=False).limit(5).toPandas())
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' жанры ' + '\033[91m' + 'в регионе' + '\033[94m' + ' EU:' + '\033[0m')
    print(genre_sales.orderBy('eu_sales', ascending=False).limit(5).toPandas())
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' жанры ' + '\033[91m' + 'в регионе' + '\033[94m' + ' JP:' + '\033[0m')
    print(genre_sales.orderBy('jp_sales', ascending=False).limit(5).toPandas())

    # График продаж в соответствии с рейтингом ESRB
    rating_sales = data.groupby('rating').agg(pyspark_sum('na_sales').alias('na_sales')).orderBy('na_sales', ascending=False)
    rating_sales.toPandas().plot(kind='bar', x='rating', y='na_sales', color='blue')
    plt.xlabel('Оценка рейтинга')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение продаж по рейтингу ESRB')
    plt.xticks(rotation=45)
    plt.show()
    plt.close()


def hypotesis_testing(data):
    print('\n\033[92m' + 'Проверка гипотез:' + '\033[0m')

    alpha = 0.25

    # Первая гипотеза
    print('\n\033[91m' + 'Гипотеза №1 (Средние значения рейтинга по платформам XBox One и PC одинаковые)' + '\033[0m')
    xbox_user_rating = data.filter(data['platform'] == 'XOne')
    xbox_mean_rating = xbox_user_rating.agg({'user_score': 'mean'}).first()[0]
    print('\033[94m' + 'Средний рейтинг по платформе XBox One: ' + '\033[0m' + f'{xbox_mean_rating}')

    pc_rating = data.filter(data['platform'] == 'PC')
    pc_mean_rating = pc_rating.agg({'user_score': 'mean'}).first()[0]
    print('\033[94m' + 'Средний рейтинг по платформе PC: ' + '\033[0m' + f'{pc_mean_rating}')

    if abs(xbox_mean_rating - pc_mean_rating) < alpha:
        print('\033[92m' + 'True' + '\033[0m')
    else:
        print('\033[91m' + 'False' + '\033[0m')

    deviation = abs(abs(xbox_mean_rating - pc_mean_rating) - alpha)
    print('\033[94m' + 'Отклонение значения от alpha: ' + '\033[0m' + f'{deviation}')

    # Вторая гипотеза
    print('\n\033[91m' + 'Гипотеза №2 (Средние значения рейтинга по жанрам Action и Sports разные)' + '\033[0m')
    action_rating = data.filter(data['genre'] == 'Action')
    action_mean_rating = action_rating.agg({'user_score': 'mean'}).first()[0]
    print('\033[94m' + 'Средний рейтинг по жанру Action: ' + '\033[0m' + f'{action_mean_rating}')

    sports_rating = data.filter(data['genre'] == 'Sports')
    sports_mean_rating = sports_rating.agg({'user_score': 'mean'}).first()[0]
    print('\033[94m' + 'Средний рейтинг по жанру Sports: ' + '\033[0m' + f'{sports_mean_rating}')

    if abs(action_mean_rating - sports_mean_rating) > alpha:
        print('\033[92m' + 'True' + '\033[0m')
    else:
        print('\033[91m' + 'False' + '\033[0m')

    deviation = abs(abs(action_mean_rating - sports_mean_rating) - alpha)
    print('\033[94m' + 'Отклонение значения от alpha: ' + '\033[0m' + f'{deviation}')


def main():
    data = data_preparation()
    exploratory_analysis(data)
    user_potret(data)
    hypotesis_testing(data)


if __name__ == '__main__':
    main()