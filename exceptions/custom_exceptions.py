class Marc2CsvException(BaseException):
    pass


class SkipDownloadButNoDb(Marc2CsvException):
    pass