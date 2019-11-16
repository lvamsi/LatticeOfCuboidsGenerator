from com.samsung.locgenerator.dao.Column import Column


class Table:
    def __init__(self, name):
        self.name = name

    def set_cols(self, cols: list(Column)):
        self.cols = cols
