import cx_Oracle

class sql(object):
    def __init__(self, username, password, hostname, service_name, port=1521):
        self.username = username
        self.password = password
        self.hostname = hostname
        self.port = port
        self.service_name = service_name

        self._connecter()

    def _connecter(self):
        #connectionString = '%s/%s@%s/%s' % (
        #    self.dbinfodict['uname'], self.dbinfodict['password'], self.dbinfodict['ip'],
        #    self.dbinfodict['service'])
        # print connectionString
        dsn = cx_Oracle.makedsn(host=self.hostname, port=self.port, service_name=self.service_name)
        self.conn = cx_Oracle.connect(user=self.username, password=self.password, dsn=dsn)

    def _executeQuery(self):
        self.cur = self.con.cursor()
        self.cur.execute(self.querystring)
        #print self.cur
        #self.cur.fetchmany(self.MAXROWS_TO_FETCH)
