import socket
import xml.etree.ElementTree as ET
import os
import configparser
from json import loads as loadjson
from time import sleep

# path = '/metadata/ngee/accepted/'
path = '/metadata/ngee/approved/'
if 'mcimac' in socket.gethostname(): path = '/metadata/ngee/test/approved/'
modifypath = '/metadata/ngee/modified/'

def listdir_nohidden(path):
    for f in os.listdir(path):
        if not f.startswith('.') and f.endswith(".xml"): # yield f
            if ET.parse(path+f).find('ome/record_id') is not None:
                yield f

def getrecordid(parsedfile):
    try:
        return parsedfile.find("ome/record_id").text
    except: pass

def getdoi(links):
    for link in links:
        if '10.5440' in link:
            yield link

class Peruser(object):
    def __init__(self, do_print, do_modify):
        self.do_print = do_print
        self.do_modify = do_modify
        self.count = 0

    def traverse(self, files, printlist, modifylist):
        for file in files:
            ftree = ET.parse(path+file)
            rid = getrecordid(ftree)
            print(rid)
            if do_print: self.printelements(printlist, ftree)
            if do_modify: self.modify(file, modifylist, ftree)

            self.setdoi(file, ftree, modifylist)
        print('[COUNT] ',self.count)

    def printelements(self, printlist, ftree):
        for i in printlist:
            element = ftree.findall(i)
            for e in element:
                print(e.text)

    def modify(self, file, modifylist, ftree):
        for i in modifylist:
            newvalue = i.get('new')
            oldpath = i.get('old')
            oldelement = ftree.find(oldpath)
            print('old: '+ oldelement.text)
            print('new: '+ newvalue)
            # oldelement.text = newvalue
        # ftree.write(modifypath+file)

    def setdoi(self, file, ftree, doipaths):
        # testfile = path + 'ZZ_MCUTesting_3_21_2018_3_49_34_PM_SOME_RENAME.NGA503.xml'
        # onlink_element = ftree.findall("idinfo/citation/citeinfo/onlink")
        onlink_element = ftree.findall(doipaths.get('old'))
        onlinks = [o.text for o in onlink_element]
        doilink = [ l for l in getdoi(onlinks) ]
        try: doilink = doilink[0]
        except Exception as e:
            print('[NO DOI FOUND] '+getrecordid(ftree))
            return
        # replacepath = 'idinfo/citation/citeinfo/lworkcit/citeinfo/onlink'
        doielement = ftree.find(doipaths.get('new'))
        if doielement is None:
            print('[DOI tag not found] ' + getrecordid(ftree))
            return
            # if 'mcimac' in socket.gethostname():
            #     parent = ftree.find('idinfo/citation/citeinfo')
            #     child = ET.Element('lworkcit')
            #     parent.append(child)
            #     parent = child
            #     child = ET.Element('citeinfo')
            #     parent.append(child)
            #     parent = child
            #     child = ET.Element('onlink')
            #     parent.append(child)
            #     child.text = '[DOI]'
            #     doielement = child
            # else:
            #     print('[DOI tag not found] '+getrecordid(ftree))
            #     return

        try:
            print('old: '+doielement.text)
            print('new: '+doilink)
            doielement.text = doilink
            ftree.write(modifypath + file)
            self.count+=1
        except: print('[FAILED] doielement text still null '+getrecordid(ftree))


        # for i in modifylist:
        #     print('[WARNING] Modifying files')
        #     time.sleep(5)
        #     element = f.findall(i)
        #     for e in element:
        #         print('changing: ' + e.text)
        #         print('to: ' +)

# Configure
config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
config.read('config.ini')


show = config['PRINT']
modify = config['MODIFY']
run = config['RUN']

do_print = run.getboolean('print')
do_modify = run.getboolean('modify')
do_doi = run.getboolean('doi')

peruser = Peruser(do_print, do_modify)

printlist = [ show[i] for i in show ] if do_print else []
modifylist = [modify[i] for i in modify] if do_modify else loadjson(modify['doi']) if do_doi else []

files = listdir_nohidden(path)

peruser.traverse(files, printlist, modifylist)

# if do_modify:
#     print('about to do modify')
#     time.sleep(5)
#     print(modifylist)
#     peruser.modify(files, modifylist)
