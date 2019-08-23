from decimal import Decimal

from file_processor.utils import check_if_feature_in_col


def default_search_check_function(line, search_query, feature,
                                  delimiter, row_data_delimiter=','):
    split_line = line.split(delimiter)

    return (
        str(search_query) == split_line[0].strip() and
        check_if_feature_in_col(split_line[1], str(feature),
                                row_data_delimiter))


DEFAULT_CONFIG = {
    # the first col in input and output files
    'label_col': None,
    'chunk_size': 1024 * 1024 * 100,
    'sorting_func':
    lambda line: Decimal(
        line.decode('utf-8').split('\t')[0].strip()),
    'search_check_function': default_search_check_function,
    'skip_first': True,
    'features': [],
    'delimiter': '\t',
    'data_row_delimiter': ',',
}
