import urllib2
import sys
import argparse
import os
import MySQLdb

VERBOSE = False

class Downloader:
    def __init__(self, host, username, password, dbname, tname, charset, start, limit, ofolder):
        self._host     = host
        self._username = username
        self._password = password
        self._db_name  = dbname
        self._tb_name  = tname
        self._charset  = charset
        self._start    = start
        self._limit    = limit
        self._output   = ofolder
        self._total    = 0
        self._success  = 0

    def _http_get(self, url):
        if VERBOSE:
            print("try to connect to url {0}".format(url))

        try:
            req = urllib2.Request(url)
            resp= urllib2.urlopen(req)
            return resp.read()
        except:
            return None

    def _download_as_pdf(self, url, name):
        with open(name, "rw+") as f:
            data = _http_get(url)
            if data is None:
                print("cannot download pdf from URL:{0}".format(url))
            else:
                if VERBOSE:
                    print("try to save file to {0}".format(name))

                f.write(data)
                self._success = self._success + 1

    def run(self):
        if VERBOSE:
            print(("try to connect to database with:\n" +
                   "host:{host}\n"                       +
                   "username:{username}\n"               +
                   "password:...\n"                      +
                   "database:{database}\n"               +
                   "charset :{charset}\n").format(host     = self._host,
                                                  username = self._username,
                                                  database = self._db_name,
                                                  charset  = self._charset))

        db = MySQLdb.connect(self._host, self._username, self._password, self._db_name, self._charset)

        cursor = db.cursor()

        sql = "select id, url from {0}".format(self._tb_name)

        if self._start is not None:
            sql = sql + " where id >= {0}".format(self._start)

        if self._limit is not None:
            sql = sql + " limit {0}".format(self._limit)

        if VERBOSE:
            f.write("try to execute SQL {0}".format(sql))

        try:
            cursor.execute(sql)

            if VERBOSE:
                f.write("try to fetch all data from database")

            ## this is not optimal, since we fetch everything into the memory but
            ## for this case it is fine since the set will not blow memory away
            results = cursor.fetchall()

            self._total = len(results)

            if VERBOSE:
                f.write("total fetched download set {0}".format(self._total))

            for r in results:
                filename = os.path.join(self._output, "{0}.download.pdf".format(r[0]))

                self._download_as_pdf(r[1], filename)
        except:
            ## this is just sloppy
            print("cannot execute SQL to database")
        finally:
            db.close()

            print("total downloaded set: {0}".format(self._total))
            print("successuflly download: {1}".format(self._success))


def _main():
    parser = argparse.ArgumentParser(description="download crap from database")
    parser.add_argument('--host' , type=str, help='host of database')
    parser.add_argument('--username', type=str, help='username of database')
    parser.add_argument('--password', type=str, help='password of database')
    parser.add_argument('--database', type=str, help='database name')
    parser.add_argument('--table', type=str, help='table name')
    parser.add_argument('--charset', type=str, help='character set of database')
    parser.add_argument('--output', type=str, help='output folder of downloaded file')

    parser.add_argument('--limit', type=int, default=20, help='limit number of downloaded')
    parser.add_argument('--start', type=int, default= 0, help='starting id')
    parser.add_argument('--verbose', type=bool, default=False, help='verbose flag')

    args = parser.parse_args()

    if args.host is None or \
       args.username is None or \
       args.password is None or \
       args.database is None or \
       args.table    is None or \
       args.charset  is None or \
       args.output   is None:
       print(parser.usage)

       return -1

    if args.verbose:
        VERBOSE = True

    Downloader(args.host, args.username, args.password, args.database, args.table, args.charset, args.start, args.limit, args.output).run()

if __name__ == "__main__":
    _main()
