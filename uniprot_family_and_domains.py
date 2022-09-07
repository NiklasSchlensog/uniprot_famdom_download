#!/usr/bin/env python3

"""
Usage: ./uniprot_family_and_domains.py <path2chunk> <outfile>

"""
import time
import os, re, sys, requests, json, csv, validators
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
    with open(f'{path_output}/assemblyid_log',"a") as f:
        if validators.url(assembly_url) != True:
            sys.stdout = f
            print(f'{assemblyid}: url does not exist')
            sys.stdout = original_stdout

        else:
            response = session.get(assembly_url)
            response.raise_for_status()
            lines = response.text.splitlines()

            if len(lines) < 2:
                sys.stdout = f
                print(f"{assemblyid}: has no Proteome_ID")
                sys.stdout = original_stdout

            elif len(lines) > 2:
                sys.stdout = f
                print(f"{assemblyid}: has more than one Proteome_ID")
                sys.stdout = original_st

            else:
                proteomeid = lines[1]
                #print(proteomeid)
                return proteomeid


def get_fandom(path_output, proteomeid, assemblyid, chunk, index):
    start = current_time()
    progress = 0

    proteome_uniprot_url = f'https://rest.uniprot.org/uniprotkb/search?fields=accession%2Cxref_interpro%2Cxref_pfam&format=tsv&query=proteome%3A{proteomeid}&size=500'
    proteome_uniparc_url = f'https://rest.uniprot.org/uniparc/search?fields=upi%2CPfam&format=tsv&query=%28{proteomeid}%29&size=500'
    r = requests.get(proteome_uniprot_url)
    q = requests.get(proteome_uniparc_url)

    if validators.url(proteome_uniprot_url) and validators.url(proteome_uniparc_url) != True:
        with open(f"{path_output}/proteomeid_log", 'a') as f:
            sys.stdout = f
            print(f"{assemblyid}|{proteomeid}: not in UniProtKB/UniParc")
            sys.stdout = original_stdout
        return index

    elif 'IPR' in r.text:
        if not os.path.exists(f"{path_output}/{chunk}/uniprotkb/"):
            os.makedirs(f"{path_output}/{chunk}/uniprotkb/")

        with open(f"{path_output}/{chunk}/uniprotkb/{assemblyid}_uniprotkb_famdom.tsv", 'w') as f:
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
            print(f"[{index}]".ljust(10)+f"{chunk}".ljust(10) + f"{assemblyid}".ljust(20) + f"{progress} / {total}".ljust(20) + f"{need}s".ljust(20))
        return index

    elif 'UPI' in q.text:
        # with open("proteome_log.txt", 'a') as f:
        #     sys.stdout = f
        #     print(f"{assemblyid}|{proteomeid}: does not exit in UniProtKB")
        #     sys.stdout = original_stdout

        if not os.path.exists(f"{path_output}/{chunk}/uniparc/"):
            os.makedirs(f"{path_output}/{chunk}/uniparc/")

        with open(f"{path_output}/{chunk}/uniparc/{assemblyid}_uniparc_famdom.tsv", 'w') as f:
            for batch, total in get_batch(proteome_uniparc_url):
                lines = batch.text.splitlines()
                if not progress:
                    print(lines[0], file=f)
                for line in lines[1:]:
                    print(line, file=f)
                progress += len(lines[1:])
            index += 1
            need = current_time()-start
            #print(f'{chunk}: [{assemblyid} | {progress} / {total}]{need}s')
            print(f"[{index}]".ljust(10)+f"{chunk}".ljust(10) + f"{assemblyid}".ljust(20) + f"{progress} / {total}".ljust(20) + f"{need}s".ljust(20))
        return index

    else:
        with open(f"{path_output}/proteomeid_log", 'a') as f:
            sys.stdout = f
            print(f"{assemblyid}|{proteomeid}: not in UniProtKB/UniParc")
            sys.stdout = original_stdout
        return index

if not os.path.exists(f"{path_output}"):
    os.makedirs(f"{path_output}")



dirs = os.listdir(f"{path_input}")
dirs = sorted(dirs)
# print(dirs)

def current_time():
    return round(time.time())


for chunk in dirs:
    index = 0
    print('Index'.ljust(10)+'Directory'.ljust(30)+'Assembly_ID'.ljust(30)+'Progress'.ljust(25)+'Time'.ljust(20))
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




