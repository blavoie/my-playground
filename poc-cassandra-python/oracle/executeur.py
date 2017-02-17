# -*- coding: utf-8 -*-

import cx_Oracle


class sql(object):
    def __init__(self, username, password, hostname, service_name, port=1521):
        self.username = username
        self.password = password
        self.hostname = hostname
        self.port = port
        self.service_name = service_name

        self._connect()

    def _connect(self):
        # connectionString = '%s/%s@%s/%s' % (
        #    self.dbinfodict['uname'], self.dbinfodict['password'], self.dbinfodict['ip'],
        #    self.dbinfodict['service'])
        # print connectionString
        dsn = cx_Oracle.makedsn(host=self.hostname, port=self.port, service_name=self.service_name)
        self.conn = cx_Oracle.connect(user=self.username, password=self.password, dsn=dsn)

        def _get_results(self):
            _list_names = []  # list of table column names
            self._all = []  # list of all dictionaries : each dictionary is column names and one row of values

            if not hasattr(self, 'result_dict'):
                self.result_dict = {}

            if self.cur.description is not None:
                for desc in self.cur.description:
                    columnName = desc[0]
                    _list_names.append(columnName)
                row, count = self.cur.fetchone(), 1

                while row and count <= self.MAXROWS_TO_FETCH:
                    self.result_dict = dict(zip(_list_names, row))  # create dict of column name: value
                    self._all.append(self.result_dict)
                    row = self.cur.fetchone()
                    count += 1

        def _print_results(self):
            count = 1
            for result in self._all:
                print 'Fetching Result no : %d' % (count)
                print '-' * 40
                for k, v in result.iteritems():
                    print '%s -> %s ' % (k, repr(v))
                count += 1
            print '-' * 40
            print 'Total results : %d' % (count - 1)
            print '-' * 40

        def _close_connection(self):
            self.conn.close()

        def executeQuery(self, querystring, no_of_records=10):
            self.MAXROWS_TO_FETCH = no_of_records
            self.querystring = querystring
            print 'Query String :::'
            print self.querystring
            self._executeQuery()
            self._getresult()
            self._print_result()
            self._close_connection()
            return self.result_dict

    if __name__ == '__main__':
        dbinfo = {'uname': 'xx', 'password': 'xxxxx', 'ip': 'x.xx.xx.xx', 'service': 'xxxx'}
        no_of_records_u_want = 1
        x = sql(dbinfo, no_of_records_u_want)  # create db connection
        # queries ----------------->
        queryString = 'select ........'
        x.executeQuery(queryString)
        x._close_connection()
