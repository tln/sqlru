"""sqlru - SQLite based LRU cache

Usage:

>>> from sqlru import lrucache
>>> d = lrucache('/tmp/lrucache.sqlite', 100)
>>> d['foo'] = ['object']

Cleanup / todo:

* Should allow multiple tables 
* Should reap periodically
* Causes errors with multiple writers
* Should allow max age
* Should allow pluggable object-to-string logic for values

"""

import sqlite3, ujson, os

class lrucache(object):
    """Expose a dict-like object which can cache json.
    """
    def __init__(self, dbfile, maxcount):
        exists = os.path.exists(dbfile)
        self.maxcount = maxcount
        self.db = sqlite3.connect(dbfile)
        if not exists:
            self.init_tables()
            
    def upd(self, sql, *parameters):
        try:
            self.db.execute(sql, parameters)
        except OperationalError:
            # assume we have write contention. re-connect. This will timeout
            # if the lock can't be acquired. Give up after this attempt
            self.db = sqlite3.connect(dbfile)
            self.db.execute(sql, parameters)
        self.db.commit()
            
    def init_tables(self):
        sql = 'create table lru(key text primary key, json text, dt text)'
        self.db.execute(sql)
        self.db.commit()
        
    def __getitem__(self, key):
        sql = 'select json from lru where key = ?'
        for [json] in self.db.execute(sql, [key]):
            # Touch the record
            sql = "update lru set dt = strftime('%Y-%m-%d %H:%M:%f', 'now') where key = ?"
            self.upd(sql, key)
            
            return ujson.loads(json)
        raise KeyError(key)

    def __setitem__(self, key, obj):
        sql = "insert or replace into lru values (?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now'))"
        json = ujson.dumps(obj)
        self.upd(sql, key, json)
        
        # select the Nth oldest date, and delete everything strictly older
        # which should leave maxcount or more records.
        sql = 'select dt from lru order by dt desc limit 1 offset %s'
        sql %= self.maxcount
        for [dt] in self.db.execute(sql):
            sql = 'delete from lru where dt < ?'
            self.upd(sql, dt)
            
if __name__ == '__main__':
    import os, sys
    os.fork()
    d = lrucache('/tmp/lrucache.sqlite', 50)
    for i in range(100):
        d[str(i)] = i
    for i in range(10, 70):
        d[str(i)] = i
    for i in range(100):
        try:
            print d[str(i)]
        except KeyError:
            pass
    
        