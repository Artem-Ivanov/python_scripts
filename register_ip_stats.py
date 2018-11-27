#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys
import csv
import sqlite3
import formz.models.db as db

from sqlobject import AND
from datetime import timedelta
from mx.DateTime.mxDateTime.mxDateTime import now

reload(sys)
sys.setdefaultencoding("utf-8")
os.chdir(os.getcwd() + '/tools/stats/temp/')

STORAGE_FILE_NAME = 'storage_ip_stats.db'
STORAGE_TABLE_NAME = 'documents'
HEADERS = (
    u'ИД', u'Тип', u'Дата создания формы', u'ФИО', u'ИНН', u'Дата рождения', u'Email', u'Телефон', u'Регион', u'Город',
    u'Заполнена ли форма до конца'
)


def errorDb(fn):
    def decorated(*args):
        try:
            result = fn(*args)
        except sqlite3.DatabaseError as err:
            print("Error: ", err)
        else:
            return result

    return decorated


class Storage:
    """Временное хранилище для форматированных записей из БД"""

    def __init__(self):
        self.conn = sqlite3.connect(STORAGE_FILE_NAME)
        self.cursor = self.conn.cursor()

        if self.checkDb() != 1:
            self.createTable()

    @errorDb
    def checkDb(self):
        """Проверка бд"""

        sql = '''
            SELECT count(*) 
            FROM sqlite_master 
            WHERE type='table' AND name='{}';
        '''.format(STORAGE_TABLE_NAME)

        query = self.cursor.execute(sql)

        return len(query.fetchall())

    @errorDb
    def createTable(self):
        """Создает таблицу для хранения документов"""

        sql = '''
            CREATE TABLE {}
            (
                id integer,
                status text,
                date text,
                fullName text,
                inn text,
                birthDay text,
                mail text,
                phone text,
                region text,
                city text,
                completed text
            )
            '''.format(STORAGE_TABLE_NAME)

        self.cursor.execute(sql)

    @errorDb
    def clear(self):
        """Удаляет записи из таблицы"""

        self.cursor.execute("DELETE FROM '{}';".format(STORAGE_TABLE_NAME))

    @errorDb
    def insert(self, docs):
        """Записывакем данные в бд"""

        self.cursor.executemany("INSERT INTO '{}' VALUES (?,?,?,?,?,?,?,?,?,?,?)".format(STORAGE_TABLE_NAME), docs)
        self.conn.commit()

    @errorDb
    def read(self):
        """Выбираем данные для отчета"""

        sql = '''
            SELECT *
            FROM {} 
        '''.format(STORAGE_TABLE_NAME)

        query = self.cursor.execute(sql)

        return query.fetchall()


class Entity:

    def __init__(self, doc, user):
        if isinstance(doc, dict):
            self.data = doc.get('data')
            self.data['user'] = user
            self.data['createdOn'] = doc.get('createdOn')
            self.data['status'] = doc.get('command')
        else:
            self.data = dict()

    def __getattr__(self, item):
        return self.data.get(item, '')

    @property
    def date(self):
        return u'{}'.format(self.data.get('createdOn').strftime('%Y-%m-%d %H:%M:%S'))

    @property
    def status(self):
        return u'Сохранен' if self.data.get('status') else u'Печать'

    @property
    def fullName(self):
        fullname = [
            self.data.get('last_name', '').title(),
            self.data.get('name', '').title(),
            self.data.get('patronymic', '').title()
        ]

        return ' '.join(fullname)

    @property
    def inn(self):
        return self.data.get('inn')

    @property
    def birthDay(self):
        return self.data.get('birth_date')

    @property
    def city(self):
        city = self.data.get('city', '').title()
        return city if city else self.data.get('borough', '').title()

    @property
    def mail(self):
        return self.data.get('email', '').lower()

    @property
    def phone(self):
        phone = u'+7{0}{1}'.format(self.data.get('phone_code', ''), self.data.get('phone', ''))
        return phone

    @property
    def region(self):
        return self.data.get('subject')

    @property
    def items(self):
        return self.data.get('items')

    @property
    def completed(self):
        props = [
            self.last_name,
            self.name,
            self.patronymic,
            self.birthDay,
            self.place_of_birth,
            self.post_code,
            self.region,
            self.city,
            self.passport_ser,
            self.passport_num,
            self.passport_date,
            self.passport_org,
            self.phone,
            self.items
        ]

        return u'Да' if all(props) else u'Нет'


class Builder:

    def __init__(self, form):
        self.form = form

    @staticmethod
    def getDataFromDatabase(formName):
        """Получить даные из бд"""
        dbClass = db.Document

        query = AND(
            dbClass.q.form == formName,
            dbClass.q.command > 0,
            dbClass.q.createdOn > now() - timedelta(days=1),
            dbClass.q.createdOn < now()
        )

        return list(dbClass.select(query))

    @staticmethod
    def parseData(docs):
        """Парсим данные для дальейшего сохранения"""

        entities = list()
        for doc in docs:
            userParam = doc.user
            entities.append(Entity(doc.sqlmeta.asDict(), userParam.sqlmeta.asDict()))
        return entities

    def saveDataInDB(self):
        """Сохраняем полученные документы в sqlite3.db файл"""

        docs = self.getDataFromDatabase(self.form)
        entities = self.parseData(docs)

        storage = Storage()
        rowList = list()
        for entity in entities:
            row = (
                entity.user.get('id'),
                entity.status,
                entity.date,
                entity.fullName,
                entity.inn,
                entity.birthDay,
                entity.mail,
                entity.phone,
                entity.region,
                entity.city,
                entity.completed
            )

            rowList.append(row)

        storage.insert(rowList)

    @staticmethod
    def saveDataInCsv():
        """Сохраняем полученные документы в .csv файл"""

        storage = Storage()
        rows = storage.read()
        storage.clear()

        with open('register_ip_stats' + '.csv', "w") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
            writer.writerows(rows)


if __name__ == '__main__':

    builder = Builder('registraciya')

    if len(sys.argv) > 1:
        methodname = sys.argv[1]
        func = getattr(builder, methodname)
        func()
    else:
        builder.saveDataInDB()
