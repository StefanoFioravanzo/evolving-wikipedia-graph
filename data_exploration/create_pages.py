"""
Author: Stefano Fioravanzo

Helper script to do some data exploration on the wikipedia revsion files

The script base behaviour is to parse a single history file and produce 
a single file for each page article with all its revisions
"""

import lxml.etree as etree
from lxml.etree import XMLParser
import fileinput
import re
import time
from pathlib import Path

DATA_FOLDER = "/Volumes/SFHDD/bigdata/wikipedia/"
ext_file_path = DATA_FOLDER + "enwiki-latest-pages-meta-history1.xml-p10p2103"

def xml_parser():
    p = XMLParser(huge_tree=True)
    # etree.register_namespace('', 'http://www.mediawiki.org/xml/export-0.10/')
    context = etree.iterparse(ext_file_path, events=("start", "end"), huge_tree=True)
    context = iter(context)
    counter = 0
    while True:
        event, elem = context.__next__()
    #     print(event)
        if event =='start' and elem.tag.split('}', 1)[1] == "page":
            print(etree.tostring(elem, encoding='unicode', method='xml'))
        # with open(f"{counter}.xml", "w") as file:
        #     file.write(etree.tostring(elem, encoding='unicode', method='xml'))
        # counter += 1

def stream_from_compressed(file_path, pages=4):
    input = fileinput.FileInput(file_path, openhook=fileinput.hook_compressed)

    # pattern for an xml tag
    tag_pattern = re.compile(r'(.*?)<(/?\w+)[^>]*?>(?:([^<]*)(<.*?>)?)?')

    write = False
    # current directory
    p = Path('./page.xml')
    page = open("page.xml", "w")
    while True:
        line = input.readline()
        # When an input file is .bz2 or .gz, line can be a bytes string
        if not isinstance(line, str): line = line.decode('utf-8')
        m = tag_pattern.search(line)
        if not m:
            continue
        tag = m.group(2)
        if tag == 'page':
            write = True
        if tag == 'title':
            title = m.group(3)
        if write:
            page.write(line)
        if tag == "/page":
            write = False
            print(f"Rename {p} to {title+p.suffix}")
            page.close()
            p.rename(Path(p.parent, title + p.suffix))
            page = open("page.xml", "w")
            pages -= 1
            if pages == 0:
                break;

    if not page.closed:
        page.close()


counter = 0
# skip the forst N articles
start_from = 0
input = fileinput.FileInput(ext_file_path)

open_pattern = re.compile(r'<page>')
close_pattern = re.compile(r'</page>')
title_pattern = re.compile(r'(<title>)(.*)(</title>)')

writing_page = False
have_title = False
# current directory
p = Path(DATA_FOLDER + 'pages/page.xml')
page = open(DATA_FOLDER + 'pages/page.xml', "w")
title = "default.xml"
# iterate file line by line (stream in, not read entirely into memory)
for line in input:

    # if we want to skip some articles, just search for
    # enclosing page tag and skip text
    if counter < start_from:
        if not writing_page:
            m = open_pattern.search(line)
            if not m:
                continue
            writing_page = True
        else:
            m = close_pattern.search(line)
            if not m:
                continue
            writing_page = False
            print(f"Skipped article {counter}")
            counter += 1
        continue

    # When an input file is .bz2 or .gz, line can be a bytes string
    # if not isinstance(line, str): line = line.decode('utf-8')
    if not writing_page:
        m = open_pattern.search(line)
        if not m:
            continue
        else:
            # found new page
            writing_page = True
            page.write(line)
    else:
        # write new page lines
        page.write(line)
        if not have_title:
            m = title_pattern.search(line)
            if m:
                print(m.string)
                # get title
                title = re.escape(m.group(2))
                have_title = True
        # end of article
        m = close_pattern.search(line)
        if not m:
            continue
        writing_page = False
        have_title = False

        # save article revisions contents in file with `articleTitle` name
        new_filename = f"{counter}-{title}{p.suffix}"
        counter += 1
        print(f"Rename {p} to {new_filename}")
        page.close()
        p.rename(Path(p.parent, new_filename))
        page = open(DATA_FOLDER + 'pages/page.xml', "w")

if not page.closed:
    page.close()

