import pymongo
import pandas as pd
from pymongo import MongoClient


def first_connection():
    client = MongoClient('mongodb://localhost:27017')
    df = pd.read_csv('games.csv')
    data = df.to_dict(orient='records')
    db = client['games']
    db_collection = db['games_collection']
    db_collection.insert_many(data)
    return db_collection


def connection():
    client = MongoClient('mongodb://localhost:27017')
    db = client['games']
    db_collection = db['games_collection']
    print('\033[92m' + '*** Success ***' + '\033[0m\n')
    return db_collection


def read_from_database(db_collection):
    all_rows = db_collection.find()
    for row in range(0, 5):
        print(all_rows[row])


def read_from_database_with_filters(db_collection, name='Tour de France 2014'):
    filtered_rows = db_collection.find({'Name': name})
    for row in filtered_rows:
        print(row)


def create_one_object(db_collection, name):
    new_row = {'Name': name, 'Platform': 'PS4', 'Year_of_Release': '2020', 'Genre': 'Sports', 'NA_sales': 0.0,
               'EU_sales': 0.0, 'JP_sales': 0.0, 'Other_sales': 0.0, 'Critic_Score': 100.0, 'User_Score': '5.1',
               'Rating': 'E'}
    db_collection.insert_one(new_row)
    print('\033[92m' + '*** Success ***' + '\033[0m')
    print('\n\033[96m' + 'Записи по фильтру "Test_game"' + '\033[0m')
    filtered_rows = db_collection.find({'Name': name})
    for row in filtered_rows:
        print(row)


def create_many_object(db_collection, names):
    new_rows = [
        {'Name': names[0], 'Platform': 'PS4', 'Year_of_Release': '2020', 'Genre': 'Sports', 'NA_sales': 0.0,
         'EU_sales': 0.0, 'JP_sales': 0.0, 'Other_sales': 0.0, 'Critic_Score': 100.0, 'User_Score': '5.1',
         'Rating': 'E'},
        {'Name': names[1], 'Platform': 'PS4', 'Year_of_Release': '2020', 'Genre': 'Sports', 'NA_sales': 0.0,
         'EU_sales': 0.0, 'JP_sales': 0.0, 'Other_sales': 0.0, 'Critic_Score': 100.0, 'User_Score': '5.1',
         'Rating': 'E'},
        {'Name': names[0], 'Platform': 'PS4', 'Year_of_Release': '2020', 'Genre': 'Sports', 'NA_sales': 0.0,
         'EU_sales': 0.0, 'JP_sales': 0.0, 'Other_sales': 0.0, 'Critic_Score': 100.0, 'User_Score': '5.1',
         'Rating': 'E'}
    ]
    db_collection.insert_many(new_rows)
    print('\033[92m' + '*** Success ***' + '\033[0m')
    print('\n\033[96m' + 'Записи по фильтру "2020"' + '\033[0m')
    filtered_rows = db_collection.find({'Year_of_Release': '2020'})
    for row in filtered_rows:
        print(row)


def edit_one(db_collection, name, platform):
    filter_condition = {'Name': name}
    update_data = {'$set': {'Platform': platform}}
    db_collection.update_one(filter_condition, update_data)
    print('\033[92m' + '*** Success ***' + '\033[0m')
    print('\n\033[96m' + 'Записи по фильтру "PS5"' + '\033[0m')
    filtered_rows = db_collection.find({'Platform': platform})
    for row in filtered_rows:
        print(row)


def edit_many(db_collection, name, platform):
    filter_condition = {'Name': name}
    update_data = {'$set': {'Platform': platform}}
    db_collection.update_many(filter_condition, update_data)
    print('\033[92m' + '*** Success ***' + '\033[0m')
    print('\n\033[96m' + 'Записи по фильтру "PS5"' + '\033[0m')
    filtered_rows = db_collection.find({'Platform': platform})
    for row in filtered_rows:
        print(row)


def delete_one(db_collection, name):
    filter_condition = {'Name': name}
    db_collection.delete_one(filter_condition)
    print('\033[92m' + '*** Success ***' + '\033[0m')
    print('\n\033[96m' + 'Записи по фильтру "2020"' + '\033[0m')
    filtered_rows = db_collection.find({'Year_of_Release': '2020'})
    for row in filtered_rows:
        print(row)


def delete_many(db_collection, name):
    filter_condition = {'Name': name}
    db_collection.delete_many(filter_condition)
    print('\033[92m' + '*** Success ***' + '\033[0m')
    print('\n\033[96m' + 'Записи по фильтру "2020"' + '\033[0m')
    filtered_rows = db_collection.find({'Year_of_Release': '2020'})
    for row in filtered_rows:
        print(row)


def main():

    # Эта функция находится под комментарием, так как используется только один раз при импортировании
    # csv файла в mongodb
    # collection = first_connection()

    print('\n\033[91m' + 'Step #1' + '\033[0m')
    print('\033[96m' + 'Подключение к базе данных' + '\033[0m')
    collection = connection()

    print('\033[91m' + 'Step #2' + '\033[0m')
    print('\033[96m' + 'Получение первых 5 записей из базы без фильтрации' + '\033[0m')
    read_from_database(collection)

    print('\n\033[91m' + 'Step #3' + '\033[0m')
    print('\033[96m' + 'Получение записей по фильтру "Tour de France 2014"' + '\033[0m')
    read_from_database_with_filters(collection)

    print('\n\033[91m' + 'Step #4' + '\033[0m')
    print('\033[96m' + 'Создание записи с именем "Test_game"' + '\033[0m')
    create_one_object(collection, 'Test_game')

    print('\n\033[91m' + 'Step #5' + '\033[0m')
    print('\033[96m' + 'Создание записи с именами "Test_game1" и "Test_game2"' + '\033[0m')
    create_many_object(collection, ['Test_game1', 'Test_game2'])

    print('\n\033[91m' + 'Step #6' + '\033[0m')
    print('\033[96m' + 'Редактирование платформы для записи с именем "Test_game2" на "PS5"' + '\033[0m')
    edit_one(collection, 'Test_game2', 'PS5')

    print('\n\033[91m' + 'Step #7' + '\033[0m')
    print('\033[96m' + 'Редактирование платформы для записей с именем "Test_game1" на "PS6"' + '\033[0m')
    edit_many(collection, 'Test_game1', 'PS6')

    print('\n\033[91m' + 'Step #8' + '\033[0m')
    print('\033[96m' + 'Удаление записи с именем "Test_game"' + '\033[0m')
    delete_one(collection, 'Test_game')

    print('\n\033[91m' + 'Step #9' + '\033[0m')
    print('\033[96m' + 'Удаление записей с именем "Test_game1"' + '\033[0m')
    delete_many(collection, 'Test_game1')

    print('\n\033[91m' + 'Step #10' + '\033[0m')
    print('\033[96m' + 'Удаление записи с именем "Test_game2"' + '\033[0m')
    delete_many(collection, 'Test_game2')


if __name__ == '__main__':
    main()
