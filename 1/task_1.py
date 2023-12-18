import json
import pickle
from pymongo import MongoClient


def connect():
    client = MongoClient()
    db = client["test-database"]
    return db.person


def get_from_pickle(filename):
    with open(filename, 'rb') as file:
        data = pickle.load(file)
    return data


def insert_many(collection, data):
    collection.insert_many(data)


def to_json(filename, data):
    filename += '.json'
    print(filename)
    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2, default=str)


def sort_by_salary(collection):
    items = []
    for person in collection.find({}, limit=10).sort({'salary': -1}): # -1 po ubivaniju
        items.append(person)
        print(person)
    to_json("sort_by_salary", items)


def filter_by_age(collection):
    items = []
    for person in (collection
            .find({"age": {"$lt": 30}}, limit=15)
            .sort({"salary": -1})):
        items.append(person)
        print(person)
    to_json("filtered_by_age", items)


def filter_by_city_and_job(collection):
    items = []
    for person in (collection
            .find({"city": "Кишинев",
                   "job": {"$in": ["Архитектор", "Косметолог", "Повар"]}},limit=10)
            .sort({"age": 1})):
        items.append(person)
        print(person)
    to_json("filtered_by_city_and_job", items)


def count_obj(collection):
    result = collection.count_documents({
        "age": {"$gt": 25, "$lt": 35}, #ne vkluchitelno
        "year": {"$gte": 2019, "$lte": 2022}, # vkluchitelno
        "$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]
    })
    data = {"count_obj": result}
    print(data)
    to_json("count_obj", data)


#data = get_from_pickle("task_1_item.pkl")
#insert_many(connect(), data)
print()
print('sort_by_salary')
sort_by_salary(connect())
print()
print('filter_by_age')
filter_by_age(connect())
print()
print('filter_by_city_and_job')
filter_by_city_and_job(connect())
print()
print('count_obj')
count_obj(connect())