# import csv
import os.path
import heapq

from file_processor.readers.reader_base import ReaderBase
from file_processor.defaults import check_if_feature_in_col


class CsvReader(ReaderBase):
    required_fields = {
        'input_fpath',
        'output_fpath',
        'training_fpath',
        'tmp_dir',
        'tmp_file',
        'sorting_func',
        'delimiter',
        'features',
    }

    def compute(self):
        self._check_config()
        # create a new file from chunks with values sorted by 1 column
        # using externak merge sort
        self._write_sorted_data_into_tmp_file()

    @staticmethod
    def _make_intermediary_file(f_path, sorted_lines):
        with open(f_path, 'wb') as f:
            for line in sorted_lines:
                f.write(line)

    def _write_sorted_data_into_tmp_file(self):
        try:
            files = [open(f_path, 'rb') for f_path in self._chunk_gen()]
            for line in heapq.merge(*files, key=self.config['sorting_func']):
                self.config['tmp_file'].write(line)
        finally:
            for f in files:
                f.close()

    def _chunk_gen(self):
        config = self.config
        with open(config['training_fpath'], 'rb') as f:
            f_count = 0
            if config['skip_first']:
                f.readline()

            lines = []
            current_size = 0

            # TODO: maybe use csv.reader()
            for line in f:
                if not any(check_if_feature_in_col(
                        line.decode('utf-8')
                        .split(config['delimiter'])[1], feature,
                        config['data_row_delimiter'])
                           for feature in config['features']):
                    continue

                line_lngth = len(line)
                current_size += line_lngth
                if current_size >= config['chunk_size'] and len(lines):
                    f_count += 1
                    f_path = os.path.join(
                        config['tmp_dir'],
                        f'{f_count}.tsv')
                    self._make_intermediary_file(
                        f_path,
                        sorted(lines, key=config['sorting_func']),
                    )
                    current_size = line_lngth
                    lines = [line]
                    yield f_path
                else:
                    lines.append(line)
            else:
                if len(lines):
                    f_path = os.path.join(
                        config['tmp_dir'],
                        f'{f_count + 1}.tsv')
                    self._make_intermediary_file(
                        f_path,
                        sorted(lines, key=config['sorting_func']),
                    )
                    yield f_path
