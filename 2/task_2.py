import json
import csv

from pymongo import MongoClient


def connect():
    client = MongoClient()
    db = client["test-database-2"]
    return db.person

def get_csv():
    data = []
    with open('task_2_item.csv', 'r', encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        for item in csv_reader:
            #job;salary;id;city;year;age
            item['salary'] = int(item['salary'])
            item['id'] = int(item['id'])
            item['year'] = int(item['year'])
            item['age'] = int(item['age'])
            data.append(item)

    return data

def insert_many(collection):
    # Добавление записей в коллекцию
    collection.insert_many(get_csv())

def to_json(filename, data):
    filename += '.json'
    print(filename)
    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2, default=str)



def get_stat_by_salary(collection):
    items = []
    q = [
        {"$group": {"_id": "result",
                    "max": {"$max": "$salary"},
                    "min": {"$min": "$salary"},
                    "avg": {"$avg": "$salary"}}}
    ]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_stat_by_salary", items)

def get_freq_by_job(collection):
    items = []
    q = [{
        "$group": {
                    "_id": "$job",
                    "count": {"$sum": 1}
                   }
          },
        {
        "$sort": {"count": -1}
         }]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_freq_by_job", items)

def get_stat_salary_by_city(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$city",
            "max": {"$max": "$salary"},
            "min": {"$min": "$salary"},
            "avg": {"$avg": "$salary"}
                    }
            },
        {
            "$sort": {"avg": -1}
        }]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_stat_salary_by_city", items)

def get_stat_salary_by_job(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$job",
            "max": {"$max": "$salary"},
            "min": {"$min": "$salary"},
            "avg": {"$avg": "$salary"}
        }
    },
        {
            "$sort": {"avg": -1}
        }]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_stat_salary_by_job", items)

def get_stat_age_by_city(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$city",
            "max": {"$max": "$age"},
            "min": {"$min": "$age"},
            "avg": {"$avg": "$age"}
        }
    },
        {
            "$sort": {"avg": -1}
        }]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_stat_age_by_city", items)

def get_stat_age_by_job(collection):
    items = []
    q = [{
        "$group": {
                   "_id": "$job",
                   "max": {"$max": "$age"},
                   "min": {"$min": "$age"},
                   "avg": {"$avg": "$age"}
        }
    },
        {
            "$sort": {"avg": -1}
        }]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_stat_age_by_job", items)

def get_max_salary_by_min_age(collection):
    items = []
    q = [{
        "$group": {"_id": "age",
                   "age": {"$min": "$age"},
                   "max_salary": {"$max": "$salary"}}}, {
        "$match": {"age": 18}}
    ]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_max_salary_by_min_age", items)

def get_min_salary_by_max_age(collection):
    items = []
    q = [{
        "$group": {"_id": "age",
                   "age": {"$max": "$age"},
                   "min_salary": {"$min": "$salary"}}}, {
        "$match": {"age": 65}}
    ]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_min_salary_by_max_age", items)

def get_50k_salary_by_avg_age(collection):
    items = []
    q = [{
        "$match": {"salary": {"$gt": 50_000}}}, {
        "$group": {"_id": "$city",
                   "max": {"$max": "$age"},
                   "min": {"$min": "$age"},
                   "avg": {"$avg": "$age"}}
            },
        {
            "$sort": {"avg": -1}
        }]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_50k_salary_by_avg_age", items)

def get_salary_by_city_job_age(collection):
    items = []
    q = [{
        "$match": {
            "city": {"$in": ["Москва", "Афины", "Вроцлав", "Мадрид"]},
            "job": {"$in": ["Программист", "Повар", "Водитель", "Инженер"]},
            "$or": [{"age": {"$gt": 18, "$lt": 25}},
                    {"age": {"$gt": 50, "$lt": 65}}]}},
        {
        "$group": {"_id": "res",
                   "max_salary": {"$max": "$salary"},
                   "min_salary": {"$min": "$salary"},
                   "avg_salary": {"$avg": "$salary"}}},
        {
        "$sort": {"count": -1}
         }]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_salary_by_city_job_age", items)

def get_salary_by_city_for_teacher(collection):
    items = []
    q = [{
        "$match": {
            "job": {"$in": ["Учитель"]},}}, {
        "$group": {"_id": "$city",
                   "max": {"$max": "$salary"},
                   "min": {"$min": "$salary"},
                   "avg": {"$avg": "$salary"}}},{
        "$sort": {"min": -1}
    }

    ]
    for item in collection.aggregate(q):
        items.append(item)
        print(item)
    to_json("get_salary_by_city_for_teacher", items)


#insert_many(connect())
print()
print('get_stat_by_salary')
get_stat_by_salary(connect())
print()
print('get_freq_by_job')
get_freq_by_job(connect())
print()
print('get_stat_salary_by_city')
get_stat_salary_by_city(connect())
print()
print('get_stat_salary_by_job')
get_stat_salary_by_job(connect())
print()
print('get_stat_age_by_city')
get_stat_age_by_city(connect())
print()
print('get_stat_age_by_job')
get_stat_age_by_job(connect())
print()
print('get_max_salary_by_min_age')
get_max_salary_by_min_age(connect())
print()
print('get_min_salary_by_max_age')
get_50k_salary_by_avg_age(connect())
print()
print('get_50k_salary_by_avg_age')
get_50k_salary_by_avg_age(connect())
print()
print('get_salary_by_city_job_age')
get_salary_by_city_job_age(connect())
print()
print('get_salary_by_city_for_teacher')
get_salary_by_city_for_teacher(connect())