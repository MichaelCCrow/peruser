#!/usr/local/bin/python3
# import socket
import xml.etree.ElementTree as ET
import os
import sys
import argparse

path = '/metadata/ngee/approved/'
# if 'mcimac' in socket.gethostname(): path = '/metadata/ngee/test/approved/'
missing = []
# printlist = [
#     'idinfo/citation/citeinfo/onlink',
#     'idinfo/citation/citeinfo/lworkcit/citeinfo/onlink',
#     'ome/record_id'
# ]

class Peruser(object):
    def __init__(self, verbose, xpath, include=None, filter=None, fromdb=False):
        self.verbose = verbose
        self.xpath = xpath
        self.include = include
        self.filter = filter
        self.fromdb = fromdb
        self.count = 0

    def traverse(self, files):
        for file in files:
            tree =  ET.fromstring(file) if self.fromdb else ET.parse(os.path.join(path, file))
            rid = getrecordid(tree)
            if self.include_in_result(tree, rid):
                quarry = tree.find(self.xpath)
                if quarry is not None:
                    quarry = quarry.text
                    print(rid+':'+quarry)
                    self.count+=1
                else: missing.append(rid)
                # if self.verbose: self.printelements(printlist, tree)

        print('[COUNT] ', self.count)
        if len(missing)>0: print('--------------------\nMissing:\n', missing)

    def include_in_result(self, tree, rid):
        if self.include is not None and not self.fromdb:
            return rid in self.include.split(',')
        if self.filter is None: return True

        for item in self.filter:
            if ':' in item:
                f = item.split(':')
                k = f[0]
                v = f[1]
                val = tree.find(k)
                if val is not None and val.text == v:
                    return True
                else: return False
            else:
                el = tree.find(item)
                exists = el is not None and not el.text.startswith('[')
                return exists

    # def printelements(self, printlist, tree):
    #     for i in printlist:
    #         element = tree.findall(i)
    #         for e in element:
    #             print(e.text)

'''Get list of xml files from directory'''
def listdir_nohidden(path):
    for f in os.listdir(path):
        if not f.startswith('.') and f.endswith('.xml'): # yield f
            if ET.parse(os.path.join(path, f)).find('ome/record_id') is not None:
                yield f

'''Get list of raw xmls strings from database'''
def querydb(include=None, verbose=False):
    import mysql.connector as sql
    conn = sql.connect(database='NGEE_Arctic_v2', user='ngeeadmin', password='ngee4db!', host='localhost')
    cursor = conn.cursor(prepared=True)
    query = 'SELECT xml FROM raw_fgdc'
    if include is not None:
        query += ' WHERE record_id in ({})'.format(str(include.split(','))[1:-1])
    if verbose: print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result

def getrecordid(parsedfile):
    el = parsedfile.find('ome/record_id')
    return el.text if el is not None else None

def getdoi(links): # ngee doi
    for link in links:
        if '10.5440' in link:
            yield link



def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-d', '--database', action='store_true', default=False,
                        help='Flag to indicate whether the xmls to be perused are to be looked up from the database. Default is false, and files are perused from approved directory.')
    parser.add_argument('-f', '--filter', action='append', required=False,
                        help='A filter for the printed results in the form key:value or a single value to check for existence.\n'
                             'Usage: peruser.py -x ome/record_id -f ome/ome_status:approved -f idinfo/citation/citeinfo/pubdate')
    parser.add_argument('-i', '--include', type=str, required=False,
                        help='Comma separated list of record_ids to include. Overrides any filters given to the -f option.')
    parser.add_argument('-v', '--verbose', default=False,
                        help='Enables more output as the script traverses the xmls')
    parser.add_argument('-x', '--xpath', type=str, required=True,
                        help='Desired outputted xpath')
    parser.add_argument('-p', '--path', required=False, type=str, default='/metadata/ngee/approved/',
                        help='Path to peruse. Default is /metadata/ngee/approved/')
    if len(sys.argv) == 1:
        parser.print_help()
        exit(0)

    args = parser.parse_args()

    global path
    path = args.path

    if args.database:
        dbxmls = querydb(include=args.include, verbose=args.verbose)
        files = [ xml[0].decode() for xml in dbxmls ]
    else: files = listdir_nohidden(path)

    peruser = Peruser(verbose=args.verbose, xpath=args.xpath, include=args.include, filter=args.filter, fromdb=args.database)
    peruser.traverse(files)

if __name__ == '__main__':
    main()