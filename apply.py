#!/usr/bin/env python3

"""
Deployment script for reporting views.
"""
# dr6

import sys
import os
import re
import argparse
import configparser
import glob
import mysql.connector

from dataclasses import dataclass
from getpass import getpass
from enum import Enum
from contextlib import closing

USAGE = """%(prog)s [-h] (--uat | --prod | --local) [-s] [-d] [-x FN] [--now] [--alter]
                FILENAME ... [-i FN ...]"""

EXPORT_ON = '''
@set maxrows -1;
@export on;
@export set {params};
'''
EXPORT_OFF = '@export off;'


SCHEMAS = ['reporting', 'warehouse', 'events']
DEFAULT_CHARSET = 'utf8'

HEADER_PTN = re.compile(r'CREATE (?:OR REPLACE )?VIEW (\S+)(?: AS)?')
WITH_PTN = re.compile(r'WITH (\S+)(?: AS)?')
MORE_WITH_PTN = re.compile(r',\s*(\S+)(?: AS)?')

class Env(Enum):
    UAT = 'uat'
    PROD = 'prod'
    LOCAL = 'local'

    @property
    def group(self):
        return self.value

# MultiSub from https://gist.github.com/khelwood/8e211f528e8383060fd5bc93be048694
class MultiSub:
    def __init__(self, subs:dict):
        self.subs = subs
        self.ptn = re.compile('|'.join(map(re.escape, subs)))

    def replacement(self, m):
        return self.subs[m.group(0)]

    def __call__(self, text:str):
        return self.ptn.sub(self.replacement, text)

@dataclass(frozen=True)
class WithDef:
    name: str
    body: str

@dataclass(frozen=True)
class ViewDef:
    name: str
    header: str
    subs: list[WithDef]
    body: str

    @classmethod
    def parse(cls, string):
        m = re.search(HEADER_PTN, string)
        name = m.group(1)
        header = m.group(0)
        string = string[m.end():].strip()
        subs, string = read_subs(string)
        body = string
        return cls(name, header, subs, body)

    def __str__(self):
        sublines = []
        if self.subs:
            joint = "WITH"
            for sub in self.subs:
                sublines.append(f"{joint} {sub.name} AS")
                sublines.append(sub.body)
                joint = ","
        combined_sub = '\n'.join(sublines)
        return '\n'.join([self.header, combined_sub, self.body])


def read_subs(string: str) -> (list, str):
    withword = 'WITH'
    withptn = WITH_PTN
    subs = []
    while True:
        while string.startswith('--'):
            i = string.index('\n')
            string = string[i+1:].lstrip()
        if not string.startswith(withword):
            return subs, string
        m = re.match(withptn, string)
        name = m.group(1)
        i = m.end()
        string = string[i:].lstrip()
        assert string.startswith('(')
        j = close_paren_index(string, 1)
        body = string[0:j+1]
        subs.append(WithDef(name, body))
        string = string[j+1:].strip()
        withword = ','
        withptn = MORE_WITH_PTN

def close_paren_index(string, start):
    in_quote = None
    in_comment = False
    in_block_comment = False
    skip = 0
    paren_count = 1
    quotes = set("`'\"")
    for i in range(start, len(string)):
        if skip:
            skip -= 1
            continue
        ch = string[i]
        if in_comment:
            if ch=='\n':
                in_comment = False
            continue
        if in_block_comment:
            if ch=='/' and string[i-1]=='*':
                in_block_comment = False
            continue
        if in_quote:
            if ch==in_quote:
                in_quote = None
            elif ch=='\\' and in_quote != '`':
                skip = 1
            continue
        if ch=='-' and string[i-1]=='-':
            in_comment = True
            continue
        if ch=='*' and string[i-1]=='/':
            in_block_comment = True
            skip = 1
            continue
        if ch in quotes:
            in_quote = ch
            continue
        if ch=='(':
            paren_count += 1
        if ch==')':
            paren_count -= 1
            if paren_count==0:
                return i
    raise ValueError("Close paren not found")

def read_files(filenames: list|None) -> (list, list):
    if not filenames:
        filenames = glob.glob('*.sql')
    nonfiles = [fn for fn in filenames if not os.path.isfile(fn)]
    if nonfiles:
        raise ValueError("Not a file: %r"%nonfiles)
    if not filenames:
        raise ValueError('No files specified.')
    contents = []
    for fn in filenames:
        with open(fn) as f:
            contents.append(f.read())
    return filenames, contents

def confirm(prompt: str) -> bool:
    print(prompt)
    while True:
        line = input('>> ').strip().lower()
        if line and 'no'.startswith(line):
            return False
        if line and 'yes'.startswith(line):
            return True

def fix_content(contents: list, config) -> list:
    sub = MultiSub({f'[{schema}]':config[schema] for schema in SCHEMAS})
    return [sub(text) for text in contents]

def apply(config, contents):
    pw = config.get('password') or getpass()
    charset = config.get('charset', DEFAULT_CHARSET)
    db_info = {
        'user': config['username'],
        'passwd': pw,
        'host': config['host'],
        'port': config.getint('port'),
        'database': config['reporting'],
        'charset': charset,
    }
    # Trying to set the collation in Python for some reason gives us an error.
    # Instead, we set the charset in direct SQL and it should
    #  default to the collation preferred by the schema.
    # NB for this to work, the schema collation should be correctly
    # set (i.e. utf8_unicode_ci)
    with closing(mysql.connector.connect(**db_info)) as con:
        with closing(con.cursor()) as cur:
            cur.execute(f"SET charset '{charset}';") # this should switch to the correct collation
            for stmt in contents:
                cur.execute(stmt)

def inline(viewdef, deps):
    subs = list(viewdef.subs)
    body = viewdef.body
    for dep in deps:
        subs += dep.subs
        subs.append(WithDef(dep.name, '(\n'+dep.body.rstrip().rstrip(';')+'\n)\n'))
    subnames = '|'.join(re.escape(sub.name) for sub in subs)
    from_index = body.index('FROM')
    body = body[:from_index] + re.sub(rf'\[[^][]+\]\.({subnames})', r'\1', body[from_index:])
    return ViewDef(viewdef.name, viewdef.header, subs, body)

INSERT_ER = '''
  INSERT INTO event_record (name, start_time) VALUES ('{0}', CURRENT_TIMESTAMP);
  SET @_er_{0} = LAST_INSERT_ID();
'''

UPDATE_ER = '''
  UPDATE event_record SET end_time=CURRENT_TIMESTAMP WHERE id=@_er_{0};
'''

def insert_er(filename, content):
    i = content.find('\nDO BEGIN')
    j = content.find('\nEND $$')
    if not (0 < i < j):
        return content
    i = content.index('\n', i+8)
    name, _ = os.path.splitext(os.path.basename(filename))
    return (content[:i] + INSERT_ER.format(name) + content[i:j]
            + UPDATE_ER + content[j:])

DELIM_1 = '\nDELIMITER $$'
DELIM_1_DBV = '\n@delimiter $$;'
DELIM_2 = '\nDELIMITER ;'
DELIM_2_DBV = '\n@delimiter ;$$'

def fix_dbvis(contents):
    if DELIM_1 in contents:
        contents = contents.replace(DELIM_1, DELIM_1_DBV)
        contents = contents.replace(DELIM_2, DELIM_2_DBV)
    return contents

def wrap_event(content, dbvis=False, now=False, alter=False):
    if not content.startswith('-- EVENT'):
        return content
    m = re.match(r'-- EVENT: OFFSET (\d+ \w+)\b', content)
    if not m:
        raise ValueError('Event config invalid')
    offset = m.group(1)
    m = re.search(r'\n-- TABLE: (\w+)\b', content)
    if not m:
        raise ValueError('Event config invalid')
    table = m.group(1)
    i = content.index('\n', m.end())+1
    content = content[i:]
    parts = []
    parts.append('@delimiter $$;' if dbvis else 'DELIMITER $$')
    event = table + ('_temp' if now else '_event')
    create = ('ALTER' if alter else 'CREATE')
    parts.append(f'{create} EVENT [reporting].{event}')
    parts.append('ON SCHEDULE')
    if now:
        parts.append('  AT CURRENT_TIMESTAMP + INTERVAL 2 MINUTE')
    else:
        parts.append('  EVERY 1 DAY')
        parts.append(f'  STARTS TIMESTAMP(CURRENT_DATE) + INTERVAL 1 DAY + INTERVAL {offset}')
    er_var = '@_er_' + table
    parts.append(f"COMMENT 'Populates the {table} table'")
    parts.append("DO BEGIN")
    parts.append('  INSERT INTO event_record (name, start_time)')
    parts.append(f"  VALUES ('{event}', CURRENT_TIMESTAMP)")
    parts.append('  ;')
    parts.append(f'  SET {er_var} = LAST_INSERT_ID();')
    parts.append(f"  TRUNCATE TABLE [reporting].{table};")
    parts.append('  '+content.strip().replace('\n', '\n  '))
    parts.append(f'  UPDATE event_record SET end_time=CURRENT_TIMESTAMP WHERE id={er_var};')
    parts.append('END $$')
    parts.append('@delimiter ;$$' if dbvis else 'DELIMITER ;')
    return '\n'.join(parts)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, usage=USAGE)
    env = parser.add_mutually_exclusive_group(required=True)
    env.add_argument('--uat', '--test', action='store_const',
        const=Env.UAT, dest='env', help='select the UAT environment')
    env.add_argument('--prod', '--production', action='store_const',
        const=Env.PROD, dest='env', help='select the production environment')
    env.add_argument('--local', action='store_const', const=Env.LOCAL,
        dest='env', help='select the local environment')
    parser.add_argument('--show', '-s', action='store_true',
        help='show the SQL without executing it')
    parser.add_argument('--dbvis', '-d', action='store_true',
        help='replace delimiter statements with those for dbvis')
    parser.add_argument('--export', '-x', action='store', metavar='FN',
        help='specify file to export data to')
    parser.add_argument('--now', action='store_true',
        help='create event to run once now instead of daily')
    parser.add_argument('--alter', action='store_true',
        help='alter event instead of creating it')
    parser.add_argument('filenames', metavar='FILENAME', nargs='+',
        help='specify SQL files')
    parser.add_argument('--inline', '-i', nargs='+', metavar='FN',
        help='views to inline')
    args = parser.parse_args()
    if args.export:
        args.show = True
    return args

def read_config():
    config = configparser.ConfigParser()
    config.read('env.ini')
    config.read('passwords.ini')
    return config

def main():
    args = parse_args()
    env = args.env
    config = read_config()
    if env.group not in config:
        sys.exit(f'Environment not found in config: {env.group}')
    config = config[env.group]
    filenames, contents = read_files(args.filenames)
    if args.inline:
        inlines = read_files(args.inline)
        inlines = [ViewDef.parse(v) for v in inlines]
        new_contents = []
        for content in contents:
            v = ViewDef.parse(content)
            v = inline(v, inlines)
            new_contents.append(str(v))
        contents = new_contents
    contents = [wrap_event(c, args.dbvis, args.now, args.alter) for c in contents]

    contents = fix_content(contents, config)

    if args.show:
        if args.export:
            params = f' filename="{args.export}"'
            if args.export.lower().endswith('.xlsx'):
                params += ' format="Excel"'
            print(EXPORT_ON.format(params=params))
        for text in contents:
            print(text)
            print()
        if args.export:
            print(EXPORT_OFF)
        return
    if (config.getboolean('check', fallback=True)
            and not confirm(f'Are you ready to update {env.name}?')):
        return
    apply(config, contents)


if __name__ == '__main__':
    main()
