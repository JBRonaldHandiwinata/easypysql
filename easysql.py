import MySQLdb
import time

from MySQLdb.cursors import DictCursor
from DBUtils.PooledDB import PooledDB


class EasySql(object):
    __pool = None
    DB_HOST = ""
    DB_NAME = ""
    DB_USER = ""
    DB_PWD = ""
    DB_PORT = 3306
    DB_CHARSET = "utf8"

    def __init__(self):
        self._conn = self.__getConn()
        self._cursor = self._conn.cursor()

    @classmethod
    def __getConn(cls):
        __pool = cls.__pool
        if cls.__pool is None:
            success = False
            count = 0
            while not success:
                try:
                    __pool = PooledDB(creator=MySQLdb, mincached=1, maxcached=100,
                                      host=cls.DB_HOST, port=cls.DB_PORT, user=cls.DB_USER, passwd=cls.DB_PWD, db=cls.DB_NAME,
                                      use_unicode=True, charset=cls.DB_CHARSET, cursorclass=DictCursor)
                    if __pool is not None:
                        success = True
                except MySQLdb.OperationalError as e:
                    if e.args[0] in (2006, 2013, 2003, 2006):
                        print("DB-CONNECTION ERROR: ", str(e.args[0]) + "-" + str(e.args[1]))
                    else:
                        print("UNKNOWN DB ERROR: ", str(e.args[0]) + "-" + str(e.args[1]))
                    success = False
                    time.sleep(2)
                    if count > 100000:
                        raise
                count += 1
        return __pool.connection()

    def fetch_rows(self, sql, values, many=0):
        self._cursor.execute(sql, values)
        retval = self._cursor.fetchone() if many == 0 else self._cursor.fetchall()
        return retval

    def insert_rows(self, tbl, colandval, on_duplicate_key_update=False, on_duplicate_key_update_condition=""):
        ct = 0
        setcol = ""
        lv = ""
        isMany = False
        if isinstance(colandval, list) or isinstance(colandval, tuple):
            isMany = True
            listtuple = list()
            for p in colandval:
                cts = 0
                vals = list()
                for key, val in p.items():
                    if key == "to":
                        key = "`to`"
                    if key == "status":
                        key = "`status`"
                    if key == "type":
                        key = "`type`"
                    if key == "from":
                        key = "`from`"
                    vals.append(val)
                    if ct == 0:
                        sep = "" if cts == (len(p.items()) - 1) else ","
                        setcol += key + sep
                        lv += "%s" + sep
                    cts += 1
                listtuple.append(tuple(vals))
                ct += 1
        elif isinstance(colandval, dict):
            vals = list()
            for key, val in colandval.items():
                if key == "to":
                    key = "`to`"
                if key == "status":
                    key = "`status`"
                if key == "type":
                    key = "`type`"
                if key == "from":
                    key = "`from`"
                vals.append(val)
                sep = "" if ct == (len(colandval.items()) - 1) else ","
                setcol += key + sep
                lv += "%s" + sep
                ct += 1
            listtuple = tuple(vals)
        else:
            raise Exception('SQL INSERT err: Wrong data type set in SECOND parameter')

        sql = "INSERT into " + tbl + " " + "(" + setcol + ")" + " VALUES " + "(" + lv + ")"
        if on_duplicate_key_update and on_duplicate_key_update_condition:
            sql += " " + "ON DUPLICATE KEY UPDATE" + " " + on_duplicate_key_update_condition
        if isMany:
            self._cursor.executemany(sql, listtuple)
        else:
            self._cursor.execute(sql, listtuple)
        return self._cursor.lastrowid

    def update_rows(self, tbl, dictset, dictwhere):
        setval = ""
        lv = ""
        if isinstance(dictset, dict):
            ct = 0
            vals = list()
            for key, val in dictset.items():
                vals.append(val)
                sep = " " if ct == (len(dictset.items()) - 1) else ", "
                setval += key + "=%s" + sep
                ct += 1
            listtuple_ds = tuple(vals)
        else:
            raise Exception('SQL UPDATE err: Wrong data type set in FIRST parameter')

        if isinstance(dictwhere, dict):
            ct = 0
            vals = list()
            for key, val in dictwhere.items():
                vals.append(val)
                sep = " " if ct == (len(dictwhere.items()) - 1) else " AND "
                lv += key + "=%s" + sep
                ct += 1
            listtuple_dw = tuple(vals)
        else:
            raise Exception('SQL UPDATE err: Wrong data type set in SECOND parameter')

        thistuple = listtuple_ds + listtuple_dw
        sql = "UPDATE " + tbl + " SET " + setval + " WHERE " + lv
        return self.__query(sql, thistuple)

    def __query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        return count

    def __getInsertId(self):
        """
        :
        :return:
        """
        self._cursor.execute("SELECT @@IDENTITY AS id")
        result = self._cursor.fetchall()
        return result[0]['id']

    def begin(self):
        """
        :keyword: Open a transaction
        :return: None
        """
        try:
            self._conn.autocommit(0)
        except:
            pass

    def end(self, option='commit'):
        """
        :keyword: Closing a transaction
        :param option: commit or rollback
        :return:
        """
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self, isEnd=1):
        """
        :keyword: Release connection pool resource
        :param isEnd: 1 or 0
        :return:
        """
        if isEnd == 1:
            self.end('commit')
        else:
            self.end('rollback')

    def closing(self):
        """
        Closing a transaction
        :return:
        """
        self._cursor.close()
        self._conn.close()
