#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import csv
import formz.models.db as db
from datetime import timedelta
from sqlobject import AND, IN
from mx.DateTime.mxDateTime.mxDateTime import now
from collections import Counter

reload(sys)
sys.setdefaultencoding("utf-8")
os.chdir(os.getcwd() + '/tools/stats/temp/')

HEADERS = [
    'Логин',
    'Эл.почта',
    'Дата регистрации',
    'Бизнес Плюс/Лайт',
    'Дата окончания подписки',
    'Последний вход в систему',
    'Общее количество документов',
    'Товарный чек',
    'Счет на оплату',
    'Расходная накладная',
    'УПД',
    'ТТН (форма 1-Т)',
    'Транспортная накладная',
    'Акт',
    'Счет-фактура',
    'Ценники',
    'ПКО',
    'РКО',
    'Платежное поручение',
    'Дата создания последнего документа',
    'Часовой пояс',
    'Организация/ ИП',
    'Наименование',
    'ИНН',
    'Адрес',
    'Эл.почта',
    'Телефон',
    'Веб-сайт',
    'Название банка',
    'БИК банка',
    'Местонахождение банка',
    'ФИО руководителя',
    'Тип учета НДС'
]


def timed(fn):
    def decorated(*args):
        start = now()
        result = fn(*args)
        print("Executing %s took %d sec" % (fn.__name__, (now() - start)))
        return result

    return decorated


class ReportConstructor:
    """Отчёт по всем пользователям Formz"""

    def __init__(self):
        self.data = ReportDataAggregator(
            loginCount=1,
            loginPeriod=30,
            docsCount=2,
            docsPeriod=60
        ).data

    @staticmethod
    def validateSubscribed(row):
        """Валидация поля subscribed"""

        subs = row.get('subscribed', '')
        subsUntil = row.get('subscribedUntil', '')

        if subs and subsUntil:
            if subsUntil > now():
                if row.get('createdOn') > now() - timedelta(days=30):
                    subs = u'Бизнес-плюс'
                else:
                    subs = u'Триал'
            else:
                subs = u'Лайт'
        else:
            subs = u'Лайт'

        row['subscribed'] = subs
        return row

    @staticmethod
    def timeZone(row):
        """Больше читабельности часовому поясу"""

        timezone = row.get('timezone', '')

        row['timezone'] = u'МСК {0:+d}'.format(timezone)
        return row

    @staticmethod
    def nds(row):
        """Валидируем значения поля НДС"""

        nds = row.get('doc_nds_type', '')

        enumDict = {
            '': u'не задано',
            '0': u'без НДС',
            '1': u'НДС в сумме',
            '2': u'НДС сверху'
        }

        row['doc_nds_type'] = enumDict.get(nds)
        return row

    @staticmethod
    def companyPhone(row):
        """Убираем лишние символы в строке телефон что бы не сбивались колонки"""

        phone = row.get('company_phone', '')

        if ',' in phone:
            phone = '; '.join(phone.split(','))

        row['company_phone'] = phone
        return row

    @staticmethod
    def statusCompany(row):
        """Валидируем статус"""

        status = row.get('company_status')

        if status:
            if not int(status):
                status = u'Организация'
            else:
                status = u'ИП'
        else:
            status = u'Физ. лицо'

        row['company_status'] = status

        return row

    @staticmethod
    def extends(row):
        """Обновляем словарь из вложенного словаря"""

        data = row.pop('data', {})
        row.update(data)

        return row

    @staticmethod
    def calculateDocsCount(userDocs, docTypes=None):
        """Возвращает количество документов переданного типа или общее количество"""

        count = 0
        if docTypes:
            for docType in docTypes:
                count += userDocs.get(docType, 0)
        else:
            count = sum(userDocs.values())

        return count

    @staticmethod
    def calculationLastLogin(logs, userId):
        """Вычисляем последний логин в систему"""

        sortedLogs = [i.createdOn for i in logs if i.userID == userId]

        return max(sortedLogs)

    @staticmethod
    def convertDictToList(dictRow):
        """Преобразуем словарь в лист перед записью"""

        listRow = list()

        sequence = (
            'login', 'email', 'createdOn', 'subscribed', 'subscribedUntil', 'lastLogin', 'documentCount',
            'check', 'invoice', 'torg12', 'upd', 'ttn', 'ttn-2011', 'act', 'sfv_2012', 'cen', 'pko', 'rko', 'platezhka',
            'lastDocCreate', 'timezone', 'company_status', 'company_name', 'company_inn', 'company_address',
            'company_email',
            'company_phone', 'company_website', 'company_bank_name', 'company_bank_bik', 'company_bank_address',
            'company_director', 'doc_nds_type'
        )

        for key in sequence:
            listRow.append(dictRow.get(key, ''))

        return listRow

    @staticmethod
    def sortedUniqueUsers(logs):
        """Из записей логов фильтрует данные пользователей возвращает уникальных"""

        userList = [log.user for log in logs]

        uniqIds = list()
        uniqUsers = list()

        for user in userList:
            if user.id not in uniqIds:
                uniqIds.append(user.id)
                uniqUsers.append(user)

        return uniqUsers

    @timed
    def prepareData(self):
        """Готовит стуктуру данных перед валидацией"""

        logs = self.data.get('logs')
        docs = self.data.get('documents')
        rawListData = list()

        validFunc = (
            self.validateSubscribed,
            self.timeZone,
            self.extends,
            self.nds,
            self.companyPhone,
            self.statusCompany,
            self.convertDictToList
        )

        for user in self.sortedUniqueUsers(logs):

            row = {
                'login': user.login,
                'email': user.email,
                'createdOn': user.createdOn,
                'subscribed': user.subscribed,
                'subscribedUntil': user.subscribedUntil,
                'lastLogin': self.calculationLastLogin(logs, user.id),
                'documentCount': self.calculateDocsCount(docs[user.id]),

                'check': self.calculateDocsCount(docs[user.id], ['check']),
                'invoice': self.calculateDocsCount(docs[user.id], ['invoice']),
                'torg12': self.calculateDocsCount(docs[user.id], ['torg12']),
                'upd': self.calculateDocsCount(docs[user.id], ['upd']),
                'ttn': self.calculateDocsCount(docs[user.id], ['ttn']),
                'ttn-2011': self.calculateDocsCount(docs[user.id], ['ttn-2011']),
                'act': self.calculateDocsCount(docs[user.id], ['act', 'ks2', 'act_sverki', 'mx1', 'torg16', 'mx3']),
                'sfv_2012': self.calculateDocsCount(docs[user.id], ['sfv_2012', 'sfv']),
                'cen': self.calculateDocsCount(docs[user.id], ['cen']),
                'pko': self.calculateDocsCount(docs[user.id], ['pko']),
                'rko': self.calculateDocsCount(docs[user.id], ['rko']),
                'platezhka': self.calculateDocsCount(docs[user.id], ['platezhka']),

                'lastDocCreate': self.calculationLastLogin(logs, user.id),
                'timezone': user.timezone,
                'data': user.data
            }

            for func in validFunc:
                row = func(row)

            rawListData.append(row)

        return rawListData

    def generate(self):
        """Подгатавливаем и записываем данные в файл"""

        rawData = self.prepareData()

        # пишем построчно в файл с заголовками
        with open('stats' + '.csv', "w") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
            writer.writerows(rawData)


class ReportDataAggregator:
    """Класс для формирования данных по заданным измирениям"""

    listDocuments = [
        'check',
        'invoice',
        'torg12',
        'upd',
        'ttn',
        'ttn_2011',
        'act',
        'ks2',
        'act_sverki',
        'mx1',
        'torg16',
        'mx3',
        'sfv_2012',
        'sfv',
        'cen',
        'pko',
        'rko',
        'platezhka'
    ]

    def __init__(self, loginCount=None, loginPeriod=None, dateStart=None, dateEnd=None, docsCount=None, docsPeriod=None,
                 listDocuments=None):
        self.logins = loginCount
        self.docsCount = docsCount
        self.loginPeriod = loginPeriod
        self.docsPeriod = docsPeriod
        self.dateStart = dateStart
        self.dateEnd = dateEnd

        self.logs = list()
        self.docs = list()

        if listDocuments:
            self.listDocuments = listDocuments

    @property
    def data(self):
        """Фомирует сагрегированные данные по заданным параметрам"""
        validUserIds = self.__getUsers()
        dictDocsCount = self.__getCountDocuments(validUserIds)
        logs = [i for i in self.logs if i.userID in validUserIds]

        return {
            'logs': logs,
            'documents': dictDocsCount
        }

    @staticmethod
    def __selectedItems(items, sortedField, minValue):
        """Отбирает значения по заданному полю и порогу вхождения"""

        countedDict = Counter([i.sqlmeta.asDict().get(sortedField) for i in items])

        return [i for i in countedDict.keys() if countedDict[i] >= minValue]

    @timed
    def __getCountDocuments(self, validUserIds):
        """Список документов c их количеством для каждого пользователя"""

        dictDocsCount = dict()

        for userId in validUserIds:
            forms = ", ".join("'{0}'".format(doc) for doc in self.listDocuments)

            query = """
                SELECT form, count(form)
                FROM document
                WHERE user_id = '{0}' AND form IN ({1})
                GROUP BY form
            """.format(userId, forms)

            query_result = db.db_conn.queryAll(query)

            dictDocsCount[userId] = {i[0]: i[1] for i in query_result}

        return dictDocsCount

    @timed
    def __getUsers(self):
        """Список пользователей для отчёта"""

        if self.loginPeriod:
            userQuery = AND(
                db.UsersLog.q.createdOn >= (now() - timedelta(days=self.loginPeriod))
            )
            self.logs = list(db.UsersLog.select(userQuery))
        else:
            self.logs = list(db.UsersLog.select())

        if self.docsPeriod:
            users = [i.userID for i in self.logs]
            docsQuery = AND(
                db.Document.q.createdOn >= (now() - timedelta(days=self.docsPeriod)),
                IN(db.Document.q.user, users)
            )
            self.docs = list(db.Document.select(docsQuery))

        userIds = self.__selectedItems(self.logs, 'userID', self.logins)
        userDocIds = self.__selectedItems(self.docs, 'userID', self.docsCount)

        if userIds and userDocIds:
            validUserId = list(set(userIds) & set(userDocIds))
        else:
            validUserId = userIds

        return validUserId


if __name__ == '__main__':
    ReportConstructor().generate()
