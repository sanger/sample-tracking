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
import argparse
import mysql.connector

from getpass import getpass
from enum import Enum
from contextlib import closing

SCHEMAS = ['reporting', 'warehouse', 'events']


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

def read_files(filenames: list|None) -> list:
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
    return contents

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
    db_info = {
        'user': config['username'],
        'passwd': pw,
        'host': config['host'],
        'port': config.getint('port'),
        'database': config['reporting'],
    }
    with closing(mysql.connector.connect(**db_info)) as con:
        with closing(con.cursor()) as cur:
            for stmt in contents:
                cur.execute(stmt)

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    env = parser.add_mutually_exclusive_group(required=True)
    env.add_argument('--uat', '--test', action='store_const', const=Env.UAT,
        dest='env', help='select the UAT environment')
    env.add_argument('--prod', '--production', action='store_const', const=Env.PROD,
        dest='env', help='select the production environment')
    env.add_argument('--local', action='store_const', const=Env.LOCAL,
        dest='env', help='select the local environment')
    parser.add_argument('--show', action='store_true',
        help="show the SQL without executing it")
    parser.add_argument('filenames', metavar='FILENAME', nargs='*',
        help='specify SQL files')
    args = parser.parse_args()
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
    contents = read_files(args.filenames)
    contents = fix_content(contents, config)
    if args.show:
        for text in contents:
            print(text)
            print()
        return
    if (config.getboolean('check', fallback=True)
            and not confirm(f'Are you ready to update {env.name}?')):
        return
    apply(config, contents)


if __name__ == '__main__':
    main()
