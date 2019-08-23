import csv
import os.path
import tempfile
from decimal import Decimal

from file_processor.defaults import DEFAULT_CONFIG
from file_processor.utils import bytes_to_string


class MainWrapper:
    def __init__(self, config={}, reader=None):
        self.config = DEFAULT_CONFIG.copy()
        self._update_config(**config)
        self.reader = reader if reader else None

    def _update_config(self, **kwargs):
        self.config.update(kwargs)

    def make_or_update_reader(self, reader=None, reader_config={}):
        merged_reader_config = self.config.copy()
        merged_reader_config.update(reader_config)
        self._update_config(**merged_reader_config)
        if reader:
            self.reader = reader(merged_reader_config)
        else:
            self.reader.config.update(merged_reader_config)

    # the class's main method
    def process_data(self, reader=None, **paths):
        """
        kwargs: 1) input_fpath; 2) output_fpath; 3) training_fpath;
        I. Perparotray steps:
           1) creates temporary directory and places temporary file there;
           2) training file is split into chunks;
           3) chunks are beinf sorted and written into files of their own;
           4) data from those files gets written into temporary file from 1)
             while being sorted via external merge sort;
        II. Data processing
           whatever you like
        """
        with tempfile.TemporaryDirectory() as tmp_dir,\
             tempfile.TemporaryFile() as tmp_file:
            self._update_config(
                **paths,
                tmp_dir=tmp_dir,
                tmp_file=tmp_file,
            )
            self.make_or_update_reader(reader=reader)
            self.reader.compute()
            tmp_file.seek(0)
            self._perform_computations()

    def _perform_computations(self):
        raise NotImplemented
