def check_if_feature_in_col(line, feature, delimiter):
    return feature == line.split(delimiter)[0]


def string_to_bytes(item, encoding='utf-8'):
    return bytes(item, encoding=encoding)


def bytes_to_string(item, encoding='utf-8'):
    return item.decode(encoding)
