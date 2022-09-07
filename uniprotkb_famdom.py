#!/usr/bin/env python3

"""
Usage: ./uniprotkb_famdom.py <path2chunk> <outfile>

"""

import time, os , re , sys, requests, json, csv, validators
from collections import defaultdict
from os.path import isfile, join
from os import listdir
from docopt import docopt
from glob import glob
from pathlib import Path
from requests.adapters import HTTPAdapter, Retry

arguments = docopt(__doc__, version="0.1")

original_stdout = sys.stdout
path_input = Path(arguments["<path2chunk>"])
path_output = Path(arguments["<outfile>"])

re_next_link = re.compile(r'<(.+)>; rel="next"')
retries = Retry(total=5, backoff_factor=0.25, status_forcelist=[501, 502, 503, 504])
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))

def get_next_link(headers):
    if "Link" in headers:
        match = re_next_link.match(headers["Link"])
        if match:
            return match.group(1)

def get_batch(batch_url):
    while batch_url:
        response = session.get(batch_url)
        response.raise_for_status()
        total = response.headers["x-total-results"]
        yield response, total
        batch_url = get_next_link(response.headers)


def get_proteome_id(assemblyid):
    assembly_url = f'https://rest.uniprot.org/proteomes/search?fields=upid&format=tsv&query={assemblyid}&size=500'
    try:
        response = session.get(assembly_url)
        response.raise_for_status()
        lines = response.text.splitlines()
        proteomeid = lines[1]
    except:
        proteomeid = None
        with open(f'{path_output}/uniprotkb/uniprotkb_log',"a") as f:
            sys.stdout = f
            print(f'{assemblyid}: not in UniProtKB')
            sys.stdout = original_stdout 

    #print(proteomeid)
    return proteomeid

#print(get_proteome_id("GCA_900128725.1"))

def get_fandom(path_output, proteomeid, assemblyid, chunk, index):
    start = current_time()
    progress = 0

    proteome_uniprot_url = f'https://rest.uniprot.org/uniprotkb/search?fields=accession%2Cxref_interpro%2Cxref_pfam&format=tsv&query=proteome%3A{proteomeid}&size=500'

    if not os.path.exists(f"{path_output}/uniprotkb/{chunk}/"):
        os.makedirs(f"{path_output}/uniprotkb/{chunk}/")
    try:
        with open(f"{path_output}/uniprotkb/{chunk}/{assemblyid}_uniprotkb_famdom.tsv", 'w') as f:
            for batch, total in get_batch(proteome_uniprot_url):
                lines = batch.text.splitlines()
                if not progress:
                    print(lines[0], file=f)
                for line in lines[1:]:
                    print(line, file=f)
                progress += len(lines[1:])
            index += 1
            need = current_time()-start
            #print((f"{chunk}/{assemblyid}\t{progress} / {total}\t{need}s").expandtabs(30))
            print(f"[{index}]".ljust(10)+f"{chunk}".ljust(20) + f"{assemblyid}".ljust(30) + f"{progress} / {total}".ljust(25) + f"{need}s".ljust(30))
    except:
        #print(f'Warning: {proteomeid} do not exit in UniprotKB')
        with open(f"{path_output}/uniprotkb/uniprotkb_log", 'a') as l:
            sys.stdout = l
            print(f"{proteomeid}: not in UniProtKB")
            sys.stdout = original_stdout

    return index


if not os.path.exists(f"{path_output}/uniprotkb/"):
    os.makedirs(f"{path_output}/uniprotkb/")



dirs = os.listdir(f"{path_input}")
dirs = sorted(dirs)
# print(dirs)

def current_time():
    return round(time.time())


for chunk in dirs:
    index = 0
    print('Index'.ljust(10)+'Directory'.ljust(20)+'Assembly_ID'.ljust(30)+'Progress'.ljust(25)+'Time'.ljust(30))
    #print(chunk)
    with open(f"{path_input}/{chunk}") as file:
        for i in file:
            assemblyid = i.replace('\n',"")
            #print(assemblyid)
            if get_proteome_id(assemblyid) != None:
                proteomeid = get_proteome_id(assemblyid)
                #print(proteomeid)
            else:
               continue

            index = get_fandom(path_output, proteomeid, assemblyid, chunk, index)

