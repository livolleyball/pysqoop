# coding=utf-8
import time
import pymysql
class NotFoundError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class OracleDatabaseError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)



class MysqlDatabaseError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def retry_if_operationalError(exception):
    print('pymysql.err.OperationalError or ConnectionResetError', time.ctime())
    return isinstance(exception, pymysql.err.OperationalError) or isinstance(exception, ConnectionResetError)
