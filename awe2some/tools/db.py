__author__ = 'lee'

import MySQLdb
import sys
from functools import wraps


class Configuration:
    def __init__(self, env):
        if env == "Prod":
            self.host = ""
        elif env == "Test":
            self.host = ""


def d2b(sql):
    _conf = Configuration(env="Prod")

    def on_sql_error(err):
        print err
        sys.exit(-1)

    def handle_sql_result(rs):
        if rs.rows > 0:
            fieldnames = [f[0] for f in rs.fields]
            return [dict(zip(fieldnames, r)) for r in rs.rows]
        else:
            return []

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            mysqlconn = MySQLdb.Connection()
            mysqlconn.settimeout(5)
            mysqlconn.connect(_conf.host, _conf.port, _conf.user, \
                              _conf.passwd, _conf.db, True, 'utf8')
            try:
                rs = mysqlconn.query(sql, {})
            except MySQLdb.Error as e:
                on_sql_error(e)

            data = handle_sql_result(rs)
            kwargs["data"] = data
            result = fn(*args, **kwargs)
            mysqlconn.close()
            return result
        return wrapper

    return decorator
