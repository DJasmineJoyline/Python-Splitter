import sys
import pandas as pd
import os
import math
import json
import xml.parsers.expat
from xml.sax.saxutils import escape



def CSVSplitter(filename):
    in_csv = filename
    number_lines = sum(1 for row in (open(in_csv,encoding="utf8")))
    #print(number_lines)
    b = os.path.getsize(filename)
    #print(b)
    gb = 1073741824
    rowsize = int(math.ceil(number_lines / math.ceil(b/gb)))
    #print(rowsize)
    count = 0
    for i in range(1,number_lines,rowsize):
        df = pd.read_csv(in_csv,header=None,nrows = rowsize,skiprows = i)
        out_csv = 'CSV' + str(count) + '.csv'
        df.to_csv(out_csv, index=False,header=False, mode='a',chunksize=rowsize) 
        count+=1
        
def JSONSplitter(filename):
    data = []
    f = open(filename)
    file_size = os.path.getsize(filename)
    for line in f:
        data.append(json.loads(line))
    if isinstance(data, list):
        data_len = len(data)
    print('Valid JSON file found')
    mb_per_file = 1024
    num_files = math.ceil(file_size/(mb_per_file*1000000))
    print('File will be split into',num_files,'equal parts')
    split_data = [[] for i in range(0,num_files)]
    starts = [math.floor(i * data_len/num_files) for i in range(0,num_files)]
    starts.append(data_len)

    for i in range(0,num_files):
        for n in range(starts[i],starts[i+1]):
            split_data[i].append(data[n])
        name = os.path.basename('JSON').split('.')[0] + '_' + str(i+1) + '.json'
        with open(name, 'w') as outfile:
            json.dump(split_data[i], outfile)
        print('Part',str(i+1),'... completed')  




CHUNK_SIZE = 1024 * 1024 * 1024
path = []
cur_size = 0
MAX_SIZE = 1024 * 1024 * 1024 # 1Gb
cur_idx = 0
cur_file = None
root = None
ext = None
start = None
ending = False



def attrs_s(attrs):
    l = ['']
    for i in range(0,len(attrs), 2):
        l.append('%s="%s"' % (attrs[i], escape(attrs[i+1])))
    return ' '.join(l)

 

def next_file():
    global cur_size, ending
    if(not ending) and (cur_size > MAX_SIZE):
        global cur_file,cur_idx
        #print("part %d Done" %cur_idx)
        ending = True
        for elem in reversed(path):
            end_element(elem[0])
        cur_file.close()
        cur_size = 0
        cur_idx += 1
        cur_file = open(root + '.%d'%cur_idx + ext, 'wt')
        for elem in path:
            start_element(*elem)
        ending = False

 
def start_element(name, attrs):
    global cur_size, start
    if start is not None:
        cur_file.write('<%s%s>' % (start[0], attrs_s(start[1])))
    start = (name, attrs)
    if ending:
        return
    cur_size += len(name) + sum(len(k) for k in attrs)
    path.append((name, attrs))
    next_file()


def end_element(name):
    global cur_size
    global start
    if start is not None:
        cur_file.write('<%s%s/>' % (start[0],attrs_s(start[1])))
    else:
        cur_file.write('</%s>' % name)
    start = None
    if ending:
        return
    if len(path) == 1:
        print (path)
    elem = path.pop()[0]
    assert elem == name
    cur_size += len(name)
    next_file()


def char_data(data):
    global cur_size, start
    if start is not None:
        cur_file.write('<%s%s>' % (start[0], attrs_s(start[1])))
        start = None
    cur_file.write(escape(data))
    cur_size += len(data)
    next_file()










def XMLSplitter(filename):
    p = xml.parsers.expat.ParserCreate()
    p.ordered_attributes = 1
    p.StartElementHandler = start_element
    p.EndElementHandler = end_element
    p.CharacterDataHandler = char_data

    global cur_file, cur_idx
    global root, ext

    root, ext = os.path.splitext(filename)
    cur_file = open(root + '.%d' % cur_idx + ext, 'wt')

    with open(filename, 'rt') as xml_file:
        while True:
            chunk = xml_file.read(1024*1024*1024)
            if chunk == '':
                break
            p.Parse(chunk)
    print ("part %d Done" % cur_idx)





ext = sys.argv[1]
fname = sys.argv[2]
if ext.lower() == "json":
    JSONSplitter(fname)
elif ext.lower() == "csv":
    CSVSplitter(fname)
elif ext.lower() == "xml":
    XMLSplitter(fname)
else:
    print("Enter valid extension( CSV/JSON/XML)")
    quit();
