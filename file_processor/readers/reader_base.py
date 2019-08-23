from file_processor.exceptions import ImproperlyConfigured


class ReaderBase:
    required_fields = {}

    def __init__(self, config={}):
        self.config = config

    def _check_config(self):
        if not self.required_fields.issubset(set(self.config.keys())):
            raise ImproperlyConfigured(
                'The following config fields are mandatory: '
                f'{", ".join(list(self.required_fields))}'
            )

    def compute(self):
        """
        the class's main function; is supposed to result in creation/filling
        of a file passed to the reader with sorted data
        """
        raise NotImplementedError
