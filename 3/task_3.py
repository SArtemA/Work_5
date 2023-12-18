import json
import msgpack

from pymongo import MongoClient


def connect():
    client = MongoClient()
    db = client["test-database-3"]
    return db.person

def get_msg():
    with open('task_3_item.msgpack', 'rb') as file:
        content = file.read()
        data = msgpack.unpackb(content)
    return data

def insert_many(collection):
    # Добавление записей в коллекцию
    collection.insert_many(get_msg())

def to_json(filename, data):
    filename += '.json'
    print(filename)
    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2, default=str)



def delete_by_salary(collection):
    res = collection.delete_many({
        "$or": [
            {"salary": {"$lt": 25_000}},
            {"salary": {"$gt": 175_000}},
        ]
    })
    print(res)

def update_age(collection):
    res = collection.update_many({}, {"$inc": {"age": 1}})
    print(res)

def increase_salary_by_job(collection):
    res = collection.update_many({
        "job": {"$in": ["Психолог", "Косметолог", "Бухгалтер"]}}, {
        "$mul": {"salary": 1.05}})
    print(res)

def increase_salary_by_city(collection):
    res = collection.update_many({
        "city": {"$in": ["Ереван", "Тарраса", "Тбилиси"]}}, {
        "$mul": {"salary": 1.07}})
    print(res)

def increase_salary_hard(collection):
    res = collection.update_many({
        "$and": [{"city": {"$in": ["Ереван", "Будапешт", "Тбилиси"]}},
                {"job": {"$in": ["Программист", "IT-специалист", "Инженер"]}},
                {"age": {"$gt": 50}}]}, {
        "$mul": {"salary": 1.1}})
    print(res)

def delete_by_city_and_job(collection):
    res = collection.delete_many({
        "$and": [
            {"city": {"$in": ["Будапешт"]}},
            {"job": {"$in": ["Программист", "IT-специалист", "Инженер"]}},
        ]
    })
    print(res)



#insert_many(connect(), data)
print()
print('delete_by_salary')
#delete_by_salary(connect())
print()
print('update_age')
#update_age(connect())
print()
print('increase_salary_by_job')
#increase_salary_by_job(connect())
print()
print('increase_salary_by_city')
#increase_salary_by_city(connect())
print()
print('increase_salary')
#increase_salary_hard(connect())
print()
print('delete_by_city_and_salary')
#delete_by_city_and_job(connect())