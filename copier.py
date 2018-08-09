import xml.etree.ElementTree as ET
import os
import csv
from shutil import copyfile
import socket

path = "/metadata/ngee/approved/"
csvfilename = 'datalinks.csv'

def listdir_nohidden(path):
    for f in os.listdir(path):
        if not f.startswith('.') and f.endswith(".xml"): # yield f
            if ET.parse(path+f).find('ome/record_id') is not None:
                yield f

def filterdoi(links):
    for link in links:
        if link.startswith('ftp://ngee.ornl.gov/data/outgoing'):
            yield link

def getimproper(links):
    for link in links:
        link = link.rstrip('/')
        if not link.endswith('data') and not link.endswith('documentation'):
            yield link

def write_csv_header():
    with open(csvfilename, 'w') as csvfile:
        cswriter = csv.writer(csvfile)
        cswriter.writerow(['RecordID'] + ['Datalinks'])

def write_to_csv(rid, onlinks):
    with open(csvfilename, 'a') as csvfile:
        cswriter = csv.writer(csvfile)
        cswriter.writerow([rid] + [onlinks])


def copy(srcfile, filename, rid):
    destination = '/var/data_operational/{}/'.format(rid)
    if not os.path.isdir(destination):
        print('making dir: '+destination)
        os.mkdir(destination)
        os.mkdir(destination+'data')
        os.mkdir(destination+'documentation')
    destination += filename
    print('copying '+srcfile+'    ->    '+destination)
    if os.path.isdir('/var/data_operational'):
        if os.path.isfile(srcfile):
            if os.path.getsize(srcfile) > 1000000000:
                with open('toolarge.txt', 'a') as tl: tl.write('record id: '+rid+' -- '+srcfile+'\n')
            else: copyfile(srcfile, destination)
        else:
            with open('errors.log', 'a') as err: err.write('record id: '+rid+' -- '+srcfile+'\n')

def analyze_links(links, rid):
    improper_links = getimproper(filterdoi(links))
    for link in improper_links:
        srcpath = link.split('/data/outgoing/')[1]
        file = link.split('/')[-1]
        # print('link: '+link)
        srcfile = '/var/data_operational/'+srcpath
        if 'mcimac' not in socket.gethostname():
            copy(srcfile, file, rid)

def traverse(files):
    write_csv_header()
    for file in files:
        f = ET.parse(path+file)
        rid = f.find("ome/record_id").text
        print(rid)
        onlink_element = f.findall("idinfo/citation/citeinfo/onlink")
        onlinks = [ o.text for o in onlink_element ]
        write_to_csv(rid, onlinks)
        analyze_links(onlinks, rid)

files = listdir_nohidden(path)
traverse(files)