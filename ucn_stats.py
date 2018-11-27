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

STORAGE_FILE_NAME = 'storage_ucn_stats.db'
STORAGE_TABLE_NAME = 'documents'
HEADERS = (
    u'ИД', u'Дата создания формы', u'Название/ФИО ИП', u'Код налогового органа', u'Объект налогообложения',
    u'Признак налогоплательщика', u'ИНН', u'КПП (для организаций)', u'ФИО руководителя', u'Телефон',
    u'Дата уведомления', u'Заполнена ли форма до конца'
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
                createdOn text,
                company_name text,
                tax_authority_code text,
                taxation_object text,
                code_indication text,
                company_inn text,
                company_kpp text,
                company_director text,
                company_phone text,
                doc_date text,
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

        self.cursor.executemany("INSERT INTO '{}' VALUES (?,?,?,?,?,?,?,?,?,?,?,?)".format(STORAGE_TABLE_NAME), docs)
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

    def __init__(self, doc):
        if isinstance(doc, dict):
            self.data = doc.get('data')
            self.extends()
        else:
            self.data = dict()

    def __getattr__(self, item):
        return self.data.get(item, '')

    @property
    def code_indication(self):

        code = self.data['code_indication']
        enum = {
            '1': 'в момент гос. регистрации',
            '2': 'в течение 30 дней с постановки на учет',
            '3': 'уход с ЕНВД',
            '4': 'переход с иных режимов кроме ЕНВД'
        }

        return enum[code]

    @property
    def taxation_objects(self):

        taxation = self.data['taxation_object']
        enum = {
            '1': 'Д',
            '2': 'Д-Р'
        }

        return enum[taxation]

    @property
    def completed(self):
        props = [
            self.data['company_name'],
            self.data['tax_authority_code'],
            self.data['company_director'],
            self.data['company_phone']
        ]

        return u'Да' if all(props) else u'Нет'

    def extends(self):
        """Обновляем словарь из вложенного словаря"""

        data = self.data.pop('data', {})
        self.data.update(data)


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
            entities.append(Entity(doc.sqlmeta.asDict()))
        return entities

    def saveDataInDB(self):
        """Сохраняем полученные документы в sqlite3.db файл"""

        docs = self.getDataFromDatabase(self.form)
        entities = self.parseData(docs)

        storage = Storage()
        rowList = list()
        for entity in entities:
            row = (
                entity.id,
                entity.createdOn,
                entity.company_name,
                entity.tax_authority_code,
                entity.taxation_object,
                entity.code_indication,
                entity.company_inn,
                entity.company_kpp,
                entity.company_director,
                entity.company_phone,
                entity.doc_date,
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

        with open('ucn_stats' + '.csv', "w") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
            writer.writerows(rows)


if __name__ == '__main__':

    builder = Builder('uvedomlenie_usn')

    if len(sys.argv) > 1:
        methodname = sys.argv[1]
        func = getattr(builder, methodname)
        func()
    else:
        builder.saveDataInDB()
