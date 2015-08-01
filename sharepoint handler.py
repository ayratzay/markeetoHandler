__author__ = 'AZaynutdinov'

import sqlalchemy
import MySQLdb
import datetime
from  requests_nltm import HttpNtlmAuth
import requests

class MysqlHandler:
    def __init__(self, hostname, username, passcode, db):
        self.hostname = hostname
        self.db_name = db
        self.db_handler = MySQLdb.connect(host=hostname, user=username, passwd=passcode, db=db, use_unicode=True,
                                          charset="utf8")

    def __repr__(self):
        return "<MySQL Handler to ('%s-%s')>" % (self.hostname, self.db_name)

    def est_connection(self):
        self.cursor = self.db_handler.cursor()

    def kill_connection(self):
        self.cursor.close()

    def disconnect_from_db(self):
        self.db_handler.close()

    def get_last_date(self, lid, aid):
        if self.cursor.execute(
                        """SELECT field from table WHERE field1 = %s AND field2 = %s
                        ORDER BY field DESC LIMIT 1""" % (lid, aid)):
            last_date = self.cursor.fetchall()[0][0]
            return (last_date - datetime.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S")
        return ("2015-08-01")

    def commit(self):
        self.db_handler.commit()


class spHandler:
    serverName = "https://yoursite.com/"
    siteName = 'sitename/'
    endpoint = "_api/web/"
    location = "/locale/path"
    local_path_ubuntu_root = "/home/"


    def __init__(self, username, passcode):
        self.passcode = passcode
        self.username = username
        self.auth = HttpNtlmAuth(username, passcode)

    def __repr__(self):
        return "<SharePoint Handler>"

    def get_token(self):
        headers = {"accept": "application/json; odata=verbose"}
        r = requests.post(self.serverName + self.siteName + "/_api/contextinfo", auth=self.auth, headers=headers)
        self.token = r.json()["d"]["GetContextWebInformation"]["FormDigestValue"]

    def download_file(self, file_name):
        r = requests.get(
            self.serverName + self.siteName + self.endpoint + "getFileByServerRelativeUrl('/" + self.location + file_name + "')/$value",
            auth=self.auth, stream=True)
        d_path = self.local_path_ubuntu_root + file_name
        with open(d_path, 'wb+') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
        return d_path


SPHandler = spHandler("userid", "passcode")
SPHandler.get_token()

