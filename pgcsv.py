#!/usr/bin/env python
import sys
import csv
import _csv
import psycopg2
import math
import cStringIO
import re
import traceback

csv.field_size_limit(524288000)

byte_counter = 0

type_rank = [
    'double precision',
    'integer',
    'character varying'
    ]

def try_type(anystring, existing):
    def current_type():
        if anystring == '' or anystring is None:
            return 'character varying'
        try:
            dummyvalue = float(anystring.replace('f',''))
            if int(anystring.replace('f','')) == dummyvalue:
                return 'integer'
            return 'float'
        except ValueError:
            return 'character varying'
    return type_rank[max((type_rank.index(current_type()),
        type_rank.index(existing)))]


class CopyProxy:

    def tell(self):
        return 0

    def seek(self, pos):
        pass

    def write(self, str):
        self.buf = self.buf + str

    def writelines(self, seq):
        self.buf = self.buf + '\n'.join(seq)

    def read(self, size):
        try:
            self._fillbuff(size)
            o = self.buf[:size]
            self.buf = self.buf[size:]
            if self.byte_counter:
                self.bytes_read += size
                if math.floor(self.bytes_read / (1024 * 1024)) > self.last_mb:
                    self.last_mb = self.bytes_read / (1024 * 1024)
                    if self.last_mb % 5 == 0:
                        print "Read %dMB" % self.last_mb
            if self.debug_file:
                self.dfh.write(o)
            return o
        except Exception:
            traceback.print_exc()
            raise

    def readline(self):
        try:
            while "\n" not in self.buf:
                self._fillbuff(1024)
            nlpos = self.buf.index('\n')
            o = self.buf[:nlpos + 1]
            self.buf = self.buf[nlpos + 1:]
            if self.debug_file:
                self.dfh.write(o)
            return o
        except Exception:
            traceback.print_exc()
            raise

    def _fillbuff(self, size):
        while len(self.buf) < size and not self.eof:
            try:
                self.writer.writerow(self.generator.next())
            except StopIteration:
                if not self.eof:
                    self.eof = True
                    if self.debug_file:
                        self.dfh.close()
                        self.debug_file = False
        
    def __init__(self, generator, byte_counter=False, debug_file=None):
        self.generator = generator
        self.buf = '' 
        self.writer = csv.writer(self, delimiter=",",
            quotechar='"', quoting=csv.QUOTE_MINIMAL, doublequote=True)
        self.byte_counter = byte_counter
        self.bytes_read = 0
        self.last_mb = 0
        self.eof = False
        self.debug_file = debug_file
        if debug_file:
            self.dfh = open(debug_file, 'w')


class PGCSV(object):

    def next(self):
        line = self.csvreader.next()
        if len(line) < self.width:
            line.extend([''] * (self.width - len(line)))
        elif len(line) > self.width:
            line = line[0:self.width]
        line = map(lambda item: item if not None else '\N', line)
        if self.strip_data:
            line = [item.strip() for item in line]
        return line
        
    def get_header(self):
        pos = self.csvfile.tell()
        self.csvfile.seek(0)
        line = self.csvreader.next()
        self.width = len(line)
        self.header = line
        self.csvfile.seek(pos)

    def init_type_dict(self):
        pos = self.csvfile.tell()
        self.csvfile.seek(0)
        header = self.next()
        self.types = ['character varying' for index, field in \
            enumerate(header)]

    def set_detect_types(self, lines):
        pos = self.csvfile.tell()
        for i in range(lines):
            line = self.next()
            for index, item in enumerate(line):
                self.types[index] = try_type(item, self.types[index])
        self.csvfile.seek(pos)

    def create_table(self):
        cur = self.conn.cursor()
        field_list = ',\n'.join(
            ["\"%s\" %s" % (field, self.types[index]) for \
            index, field in enumerate(self.header)]
            )
        
        table_query = """
            CREATE TABLE \"%s\" (
                %s
            );
            """ % (self.table_name, field_list)
        if self.drop_first:
            cur.execute("DROP TABLE IF EXISTS \"%s\";" % self.table_name)
        cur.execute(table_query)
        self._do_copy(cur)
        self.conn.commit()

    def _do_copy(self, cursor):
        cp = CopyProxy(self, byte_counter=self.byte_counter,
            debug_file=self.debug_file)
        cursor.copy_expert(
            'COPY "%s" FROM STDIN WITH DELIMITER AS'
            '\',\' CSV QUOTE AS \'"\'' % \
            self.table_name, cp, 1024)

    def _clean_name(self, s):
        s = s.lower().strip()
        s = re.sub('[- ]', '_', s)
        s = re.sub('\W', '', s)
        return s

    def _dedupe_names(self, fields):
        outfields = list(fields)
        for index, field in enumerate(fields):
            appendage = 1
            while field in outfields[:index] and \
                outfields[index] in outfields[:index]:
                appendage += 1
                outfields[index] = fields[index] + str(appendage)
        return outfields

    def __init__(self, csvfile, table_name, pg_service, strip_data=False,
        detect_types=False, dialect='excel', sniff_dialect=False,
        schema='public', dialect_bytes=2048, drop_first=False,
        clean_field_names=False, detect_type_lines=100, byte_counter=False,
        debug_file=None, force_tabbed=False, **fmtparam):

        self.drop_first = drop_first
        self.table_name = table_name
        self.pg_service = pg_service
        self.detect_types = detect_types
        self.schema = schema
        self.csvfile = csvfile
        self.sniff_dialect = sniff_dialect
        self.strip_data = strip_data
        if sniff_dialect:
            try:
                pos = self.csvfile.tell()
                self.csvfile.seek(0)
                sd = self.csvfile.read(dialect_bytes)
                dialect = csv.Sniffer().sniff(sd, ',\t')
                self.csvfile.seek(pos)
            except _csv.ERror:
                dialect=None
        if force_tabbed:
            csv.register_dialect('tabbed', delimiter="\t", quoting=csv.QUOTE_NONE)
            dialect = 'tabbed'
        self.csvreader = csv.reader(csvfile, dialect=dialect, **fmtparam)
        self.get_header()
        self.init_type_dict()
        if clean_field_names:
            self.header = [self._clean_name(item) for item in self.header]
            self.header = self._dedupe_names(self.header)

        self.conn = psycopg2.connect(pg_service)
        
        if detect_types:
            self.set_detect_types(detect_type_lines)
        self.byte_counter = byte_counter
        self.debug_file = debug_file

def main():
    import argparse
    parser = argparse.ArgumentParser(
        "Import a CSV into a PostgreSQL database.")
    parser.add_argument('-f','--filename', metavar='FILENAME', 
        dest='csvfile', required=True)
    parser.add_argument('-t','--table-name', metavar='TABLENAME',
        dest='table_name', required=True)
    parser.add_argument('-T','--detect-types', action='store_true',
        dest='detect_fieldtypes')
    parser.add_argument('-c', '--clean-fields', action='store_true',
        dest='clean_fields')
    parser.add_argument('-s', '--strip-data', action='store_true',
        dest='strip_data')
    parser.add_argument('-a', '--auto-detect', action='store_true',
        dest='auto_detect', help='Autodetect delimiters (Default to [,"].')
    parser.add_argument('-p', '--conninfo', metavar='CONNINFO',
        dest='conninfo')
    parser.add_argument('-d', '--drop-first', action='store_true',
        dest='drop_first')
    parser.add_argument('-C', '--byte-counter', action='store_true',
        dest='byte_counter')
    parser.add_argument('--debug-file', dest='debug_file')
    parser.add_argument('-r', '--force-tabbed', dest='force_tabbed')

    args = parser.parse_args()
    
    if args.csvfile != '-':
        fo = open(args.csvfile, 'Ur')
    else:
        fo = sys.stdin

    pgcsv = PGCSV(fo, args.table_name, args.conninfo,
        strip_data=args.strip_data, detect_types=args.detect_fieldtypes,
        sniff_dialect=args.auto_detect, drop_first=args.drop_first,
        clean_field_names=args.clean_fields, byte_counter=args.byte_counter,
        debug_file=args.debug_file, force_tabbed=args.force_tabbed)
    
    pgcsv.create_table()

if __name__ == "__main__":
    main()