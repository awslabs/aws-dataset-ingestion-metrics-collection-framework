"""Dataset"""

class Dataset():
    """
    Represent a single DataSet in lake catalog
    """
    catalog: str
    database: str
    table: str
    alias: str
    def __init__(self, database, table, alias='', catalog=''):
        self.database = database
        self.table = table
        self.catalog = catalog
        if alias == '':
            self.alias = table
        else:
            self.alias = alias
