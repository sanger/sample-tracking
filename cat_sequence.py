#!/usr/bin/env python3

"""
Script to concatenate patches from the patch sequence file.

E.g.
 $ ./cat_patches.py schema --from Production --to 2.4
 Concatenate schema patches from where Production occurs in the
 sequence file, to where 2.4 occurs.
Expected categories are data, static, schema and view.
Each patch is preceded by an insert into the db_history table.
"""

from __future__ import annotations

import sys
import os
import argparse
from typing import Optional, Union, Generator, overload

class Entry:
    def __init__(self, comment: Optional[str], filename: Optional[str]):
        self.comment = comment
        self.filename = filename

    def has_category(self, categories:Optional[tuple]) -> bool:
        if categories is None:
            return True
        return self.filename is not None and self.filename.startswith(categories)

    def __str__(self):
        if not self.filename:
            return self.comment
        if not self.comment:
            return self.filename
        return self.comment + '\n' + self.filename

    def __contains__(self, string: str):
        return any(x and string in x for x in (self.comment, self.filename))

class EntrySequence:
    def __init__(self, entries: list[Entry]):
        self.entries = entries

    def __len__(self):
        return len(self.entries)

    @classmethod
    def load(cls, filename:str) -> EntrySequence:
        return cls(list(load_entries(filename)))

    def find_string(self, string:str, from_index:int=0) -> int:
        if from_index==0:
            return next((i for (i, entry) in enumerate(self) if string in entry), -1)
        return next((i for i in range(from_index, len(self)) if string in self.entries[i]), -1)

    def index_to_int(self, index:Union[str,int], from_index:int=0) -> int:
        if isinstance(index, int):
            return index
        if index is None:
            return None
        if not isinstance(index, str):
            raise ValueError("illegal index %r"%index)
        i = self.find_string(index, from_index)
        if i >= 0:
            return i
        if from_index==0:
            raise ValueError("string not found: %r"%index)
        raise ValueError("string not found after start point: %r"%index)

    def __iter__(self):
        "Unnecessary, but mypy cannot cope without it."
        return iter(self.entries)

    @overload
    def __getitem__(self, index: slice) -> EntrySequence: ...
    @overload
    def __getitem__(self, index: Union[int,str]) -> Entry: ...
    def __getitem__(self, index):
        if isinstance(index, slice):
            start = self.index_to_int(index.start)
            stop = self.index_to_int(index.stop, start or 0)
            return EntrySequence(self.entries[slice(start, stop, index.step)])
        return self.entries[self.index_to_int(index)]

    def limit(self, limit: int) -> EntrySequence:
        if len(self) < limit:
            return self
        if limit <= 0:
            return EntrySequence([])
        entries = self.entries
        i = 0
        while i < len(entries):
            e = entries[i]
            i += 1
            if e.filename:
                limit -= 1
                if limit <= 0:
                    break
        if i >= len(entries):
            return self
        return EntrySequence(entries[:i])

    def filter_categories(self, categories: list) -> EntrySequence:
        if not categories:
            return self
        categories_tuple = tuple(categories)
        return EntrySequence([x for x in self if x.has_category(categories_tuple)])

    def check(self) -> None:
        missing = [fn for fn in (e.filename for e in self)
                   if fn and not os.path.isfile(fn)]
        if missing:
            raise ValueError("Not a file: %r"%missing)

    def print_list(self) -> None:
        if not self:
            print('No entries')
        for e in self:
            print(e)

    def cat(self, check:bool=True) -> None:
        for e in self:
            if e.filename:
                cat_patch_file(e.filename, check)

def load_entries(filename: str) -> Generator[Entry, None, None]:
    comments:list[str] = []
    with open(filename, 'r') as fin:
        for line in fin:
            line = line.strip()
            if not line:
                if comments:
                    yield Entry('\n'.join(comments), None)
                    comments = []
                continue
            if line.startswith('#'):
                comments.append(line)
                continue
            filename = line
            yield Entry('\n'.join(comments) if comments else None, filename)
            comments = []
        if comments:
            yield Entry('\n'.join(comments), None)

def log_patch_filename(filename:str) -> None:
    print(f"\n-- DB history entry for {filename}")
    print(f"INSERT INTO db_history (filename) VALUES ('{filename}');")
    print("SET @current_patch_id=LAST_INSERT_ID();")
    print()

def cat_patch_file(filename:str, check:bool=True) -> None:
    log_after = (filename=='schema/db_history.sql')
    if not log_after:
        log_patch_filename(filename)

    with open(filename) as fin:
        data = fin.read().strip()

    if check:
        try:
            data.encode('ascii')
        except UnicodeEncodeError:
            sys.stderr.write("\nNon-ASCII characters in file "+filename)
            raise
    print(data)

    if log_after:
        log_patch_filename(filename)

def parse_args() -> argparse.Namespace:
    doc, _, epilog = __doc__.partition('\n\n')
    parser = argparse.ArgumentParser(
        description=doc.strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog.strip())
    fileorpatches = parser.add_mutually_exclusive_group()
    fileorpatches.add_argument('--file', '-f', default='sequence.txt',
                               help='sequence file to read (default sequence.txt)')
    fileorpatches.add_argument('--patches', nargs='+',
                               help='use given patches instead of the ones in the sequence file')
    parser.add_argument('categories', nargs='*',
                        help='what categories of patch to include')
    parser.add_argument('--from', dest='start', help='name of patch to start at')
    parser.add_argument('--to', dest='stop', help='name of patch to stop before')
    parser.add_argument('-n', '--limit', type=int,
                        help='limit the number of patches to include')
    parser.add_argument('--list', action='store_true', help='just list patches')
    parser.add_argument('--nocheck', dest='check', action='store_false',
                        help='do not check patches exist')
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    if args.patches:
        entries = [Entry(None, filename) for filename in args.patches]
        sequence = EntrySequence(entries)
    else:
        sequence = EntrySequence.load(args.file)
    if args.check:
        sequence.check()
    sequence = sequence[args.start:args.stop]
    if args.limit is not None:
        sequence = sequence.limit(args.limit)
    sequence = sequence.filter_categories(args.categories)
    if args.list:
        sequence.print_list()
    else:
        sequence.cat(args.check)


if __name__ == '__main__':
    main()
