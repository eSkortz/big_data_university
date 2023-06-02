# Импортируем pandas для обработки csv файлов
import pandas as pd
# Импортируем numpy для обработки числовых значений
import numpy as np
# Импортируем matplotlib для построения графиков
import matplotlib.pyplot as plt
# Импортируем модуль экспоненциального сглаживания из statsmodels для построения несложных прогнозов
from statsmodels.tsa.holtwinters import ExponentialSmoothing


def data_preparation():
    data = pd.read_csv('games.csv')

    print('\n\033[92m' + 'Подготовка данных' + '\033[0m\n')

    # Преобразуем названия столбцов в lowercase
    data.columns = data.columns.str.lower()

    print('\033[91m' + 'Названия столбцов для проверки преобразования в нижний регистр:'
          + '\033[0m' + f'\n{data.columns}\n')

    # Заменяем тип данных на int в столбце year_of_release, заменяя nan на 0, так как год выпуска является целочисленным
    # значением
    data['year_of_release'] = data['year_of_release'].fillna(0).astype(int)
    print('\033[91m' + 'Уникальные значения в столбце' + '\033[94m' + ' "year_of_release" ' + '\033[91m'
          + 'для проверки преобразования типов:\n' + '\033[0m' + f'{data["year_of_release"].unique()}\n')

    # Заменяем 'tbd' значения в столбце user_score для корректного преобразования и последующей обработки
    data['user_score'] = data['user_score'].replace('tbd', np.nan)
    # Заменяем тип данных в столбце user_score на float, заменяя nan на 0, так как оценка пользователей является float
    # параметром
    data['user_score'] = data['user_score'].fillna(0.0).astype(float)
    print('\033[91m' + 'Уникальные значения в столбце' + '\033[94m' + ' "user_score" ' + '\033[91m' +
          'для проверки преобразования типов:\n' + '\033[0m' + f'{data["user_score"].unique()}\n')

    # Заменяем nan значения в столбце critic_score и преобразуем его в целочисленный тип, так как оценка критиков
    # является целочисленным значением от 1 до 100
    data['critic_score'] = data['critic_score'].fillna(0).astype(int)
    print('\033[91m' + 'Уникальные значения в столбце' + '\033[94m' + ' "critic_score" ' + '\033[91m'
          + 'для проверки преобразования типов:\n' + '\033[0m' + f'{data["critic_score"].unique()}\n')

    # Преобразуем пустые значения в столбце, который расшифровывается как "Rating Pending"
    data['rating'] = data['rating'].fillna('RP')

    # Считаем сумму всех продаж и записываем ее в отдельный столбец
    data['sum_sales'] = data[['na_sales', 'eu_sales', 'jp_sales', 'other_sales']].sum(axis=1)

    # Удаляем записи, в которых есть nan значения в столбцах name и genre (в нашем случае их две)
    data = data.dropna(subset=['name'])

    # Подводим итоги подготовки файла к работе
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "name": ' +
          '\033[0m' + f'{data["name"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "platform": ' +
          '\033[0m' + f'{data["platform"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "year_of_release": ' +
          '\033[0m' + f'{data["year_of_release"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "genre": ' +
          '\033[0m' + f'{data["genre"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "na_sales": ' +
          '\033[0m' + f'{data["na_sales"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "eu_sales": ' +
          '\033[0m' + f'{data["eu_sales"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "jp_sales": ' +
          '\033[0m' + f'{data["jp_sales"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "other_sales": ' +
          '\033[0m' + f'{data["other_sales"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "critic_score": ' +
          '\033[0m' + f'{data["critic_score"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "user_score": ' +
          '\033[0m' + f'{data["user_score"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "rating": ' +
          '\033[0m' + f'{data["rating"].isnull().sum()}')
    print('\033[91m' + 'Кол-во NaN значений в столбце' + '\033[94m' + ' "sum_sales": ' +
          '\033[0m' + f'{data["sum_sales"].isnull().sum()}\n')

    # Возращаем преобразованные данные для дальнейшей их обработки
    return data


def exploratary_analysis(data):
    print('\033[92m' + 'Исследовательский анализ данных' + '\033[0m\n')

    print('\033[91m' + 'Кол-во игр выпущенных в период' + '\033[94m' + ' с 1980 по 1990: ' + '\033[0m' +
          f'{len(data[(data["year_of_release"] >= 1980) & (data["year_of_release"] <= 1990)])}')
    print('\033[91m' + 'Кол-во игр выпущенных в период' + '\033[94m' + ' с 1990 по 2000: ' + '\033[0m' +
          f'{len(data[(data["year_of_release"] >= 1990) & (data["year_of_release"] <= 2000)])}')
    print('\033[91m' + 'Кол-во игр выпущенных в период' + '\033[94m' + ' с 2000 по 2010: ' + '\033[0m' +
          f'{len(data[(data["year_of_release"] >= 2000) & (data["year_of_release"] <= 2010)])}')
    print('\033[91m' + 'Кол-во игр выпущенных в период' + '\033[94m' + ' с 2010 по 2020: ' + '\033[0m' +
          f'{len(data[(data["year_of_release"] >= 2010) & (data["year_of_release"] <= 2020)])}\n')

    # Составляем список платформ по продажам и соритруем его по убыванию
    platform_sales = data.groupby('platform')['sum_sales'].sum().sort_values(ascending=False)
    # Выбираем верхние 5 строчек из списка
    top_platforms = platform_sales.head(5).index
    print('\033[91m' + 'Топ 5 платформ по продажам игр: ' + '\033[0m' + f'{top_platforms}\n'
          + '\033[94m' + 'Более подробные данные можно увидеть на всплывающем графике' + '\033[0m\n')
    # Делаем выборку из исходных данных только по выбранным 5 платформам
    filtered_data = data[data['platform'].isin(top_platforms)]
    # Строим распределение по годам для каждой платформы
    platform_sales_by_year = filtered_data.groupby(['platform', 'year_of_release'])['sum_sales'].sum().unstack()

    # Визуализируем полученные данные
    platform_sales_by_year.plot(kind='bar', stacked=True)
    plt.xlabel('Год выпуска')
    plt.ylabel('Суммарные продажи')
    plt.title('Продажи по платформам по годам')
    plt.legend(loc='upper left')
    plt.show()
    plt.close()

    # Выполняем прогноз по продажам на 2017 год на основе данных за актуальный период 2010-2016 год
    actual_period_data = data[(data["year_of_release"] >= 2010) & (data["year_of_release"] <= 2016)]
    sales_data = actual_period_data.groupby('year_of_release')['sum_sales'].sum()

    # Производим вычисление разницы в продажах между годами и делаем прогноз на 2017 год
    sales_mas = []
    for counter in range(2010, 2015):
        sales_mas.append((sales_data[sales_data.index == counter + 1].values[0])
                         - (sales_data[sales_data.index == counter].values[0]))
    sales_array = np.array(sales_mas)
    print('\033[91m' + 'Прогнозируемые продажи на 2017 год на основе предыдущих годов: ' + '\033[0m' +
          f'{sales_data[sales_data.index == 2016].values[0] + sales_array.mean()}\n')

    # Составляем топ платформ по продажам
    platform_sales = data.groupby('platform')['sum_sales'].sum().sort_values(ascending=False)
    print('\033[91m' + 'Продажи по топ-5 платформам:' + '\033[0m')
    # Берем первые пять значений по списку для обработки
    for counter in range(0, 5):
        # Смотрим на продажи по платформе в 2015 и 2016 году соответственно
        actual_data_first = data[(data['year_of_release'] == 2016) & (data['platform'] == platform_sales.index[counter])]
        actual_data_second = data[(data['year_of_release'] == 2015) & (data['platform'] == platform_sales.index[counter])]
        actual_data_first_sum = actual_data_first['sum_sales'].sum()
        actual_data_second_sum = actual_data_second['sum_sales'].sum()
        if actual_data_first_sum > actual_data_second_sum:
            parameter = 'Возрастает'
        else:
            parameter = 'Убывает'
        # Выводим полученные данные
        print(f'{platform_sales.index[counter]} - {platform_sales[counter]} - {parameter}')

    # График с усами по продажам на определенные платформы
    # Делаем выборку по платформам
    platform_sales = data.groupby('platform')['sum_sales'].sum()
    # Соритируем по продажам в порядке убывания
    sorted_platforms = platform_sales.sort_values(ascending=False)
    # Берем первые 5 значений
    top_platforms = sorted_platforms.head(5)
    # Компонуем данные для построения боксплота
    sales_data = [data[data['platform'] == platform]['sum_sales'] for platform in top_platforms.index]
    # Строим график
    plt.boxplot(sales_data, labels=top_platforms.index)
    plt.xlabel('Платформа')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение глобальных продаж игр по платформам')
    plt.show()
    plt.close()

    # Влияние отзывов критиков и пользователей на продажи игр
    platform_data = data[data['platform'] == 'PS2']
    platform_data = platform_data.dropna(subset=['user_score', 'critic_score'])
    # Построение диаграммы рассеяния
    plt.scatter(platform_data['user_score']*10, platform_data['sum_sales'], label='Отзывы пользователей')
    plt.scatter(platform_data['critic_score'], platform_data['sum_sales'], label='Отзывы критиков')
    plt.xlabel('Отзывы')
    plt.ylabel('Глобальные продажи')
    plt.title('Влияние отзывов на продажи - платформа PS2')
    plt.legend()
    plt.show()
    plt.close()
    # Вывод матрицы корреляции
    correlation = platform_data[['user_score', 'critic_score', 'sum_sales']].corr()
    print('\n\033[91m' + 'Матрица корреляции по продажам и оценкам:' + '\033[0m\n')
    print(correlation)

    # Группируем данные по жанру и суммируем продажи
    genre_sales = data.groupby('genre')['sum_sales'].sum().sort_values(ascending=False)

    # Строим график столбцов для распределения продаж по жанрам
    genre_sales.plot(kind='bar', color='blue')
    plt.xlabel('Жанр')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение продаж по жанрам')
    plt.xticks(rotation=45)
    plt.show()
    plt.close()

    # Выделяем самые прибыльные жанры
    top_genres = genre_sales.head(5)
    print('\n\033[91m' + 'Самые прибыльные жанры:' + '\033[0m')
    print(top_genres.head())

    print('\n\033[91m' + 'Жанры с высокими продажами:' + '\033[0m')
    print(genre_sales.head())

    print('\n\033[91m' + 'Жанры с низкими продажами:' + '\033[0m')
    print(genre_sales.tail())


def user_potret(data):

    print('\n\033[92m' + 'Портрет пользователя в регионах:' + '\033[0m')

    # Самые популярные платформы в регионах
    platform_sales = data.groupby('platform')[['na_sales', 'eu_sales', 'jp_sales']].sum()
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' платформы ' + '\033[91m' + 'в регионе' + '\033[94m'
          + ' NA:' + '\033[0m')
    print(platform_sales.sort_values(['na_sales'], ascending=False).head())
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' платформы ' + '\033[91m' + 'в регионе' + '\033[94m'
          + ' EU:' + '\033[0m')
    print(platform_sales.sort_values(['eu_sales'], ascending=False).head())
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' платформы ' + '\033[91m' + 'в регионе' + '\033[94m'
          + ' JP:' + '\033[0m')
    print(platform_sales.sort_values(['jp_sales'], ascending=False).head())

    # Самые популярные жанры в регионах
    genre_sales = data.groupby('genre')[['na_sales', 'eu_sales', 'jp_sales']].sum()
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' жанры ' + '\033[91m' + 'в регионе' + '\033[94m' + ' NA:'
          + '\033[0m')
    print(genre_sales.sort_values(['na_sales'], ascending=False).head())
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' жанры ' + '\033[91m' + 'в регионе' + '\033[94m' + ' EU:'
          + '\033[0m')
    print(genre_sales.sort_values(['eu_sales'], ascending=False).head())
    print('\n\033[91m' + 'Самые популярные' + '\033[94m' + ' жанры ' + '\033[91m' + 'в регионе' + '\033[94m' + ' JP:'
          + '\033[0m')
    print(genre_sales.sort_values(['jp_sales'], ascending=False).head())

    # График продаж в соответствии с рейтингом ESRB
    rating_sales = data.groupby('rating')[['na_sales']].sum().sort_values('na_sales', ascending=False)
    rating_sales.plot(kind='bar', color='blue')
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
    print('\n\033[91m' + 'Гипотеза №1 (Средние значения рейтинга по платформам XBox One и Pc одинаковые)' + '\033[0m')
    xbox_user_rating = data[data['platform'] == 'XOne']
    print('\033[94m' + 'Средний рейтинг по платформе XBox One: '
          + '\033[0m' + f'{xbox_user_rating["user_score"].mean()}')
    pc_rating = data[data['platform'] == 'PC']
    print('\033[94m' + 'Средний рейтинг по платформе PC: '
          + '\033[0m' + f'{pc_rating["user_score"].mean()}')
    if abs(pc_rating["user_score"].mean() - xbox_user_rating["user_score"].mean()) < alpha:
        print('\033[92m' + 'True' + '\033[0m')
    else:
        print('\033[91m' + 'False' + '\033[0m')
    print('\033[94m' + 'Отклонение значения от alpha: ' + '\033[0m' +
          f'{abs(abs(pc_rating["user_score"].mean() - xbox_user_rating["user_score"].mean()) - alpha)}')

    # Вторая гипотеза
    print('\n\033[91m' + 'Гипотеза №2 (Средние значения рейтинга по жанрам action и sports разные)' + '\033[0m')
    action_rating = data[data['genre'] == 'Action']
    print('\033[94m' + 'Средний рейтинг по жанру Action: '
          + '\033[0m' + f'{action_rating["user_score"].mean()}')
    sports_rating = data[data['genre'] == 'Sports']
    print('\033[94m' + 'Средний рейтинг по жанру Sports: '
          + '\033[0m' + f'{sports_rating["user_score"].mean()}')
    if abs(action_rating["user_score"].mean() - sports_rating["user_score"].mean()) > alpha:
        print('\033[92m' + 'True' + '\033[0m')
    else:
        print('\033[91m' + 'False' + '\033[0m')
    print('\033[94m' + 'Отклонение значения от alpha: ' + '\033[0m' +
          f'{abs(abs(action_rating["user_score"].mean() - sports_rating["user_score"].mean()) - alpha)}')


def main():
    data = data_preparation()
    exploratary_analysis(data)
    user_potret(data)
    hypotesis_testing(data)


if __name__ == '__main__':
    main()
