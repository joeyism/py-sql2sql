import logging

class ETL(object):
    from_conn = None
    to_conn = None
    extract_query = None
    transform_function = None
    load_query = None

    logger = logging.getLogger("etl")

    def __init__(self, log_level=0):
        self.logger.setLevel(log_level)

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def from_conn(self, conn):
        self.from_conn = conn
        return self

    def to_conn(self, conn):
        self.to_conn = conn
        return self

    def extract(self, statement, tup=()):
        self.extract_query = (statement, tup)
        return self

    def transform(self, f):
        self.transform_function = f
        return self

    def load(self, statement, tup=()):
        self.load_query = (statement, tup)
        return self

    def execute(self, before_extract=None, after_extract=None, before_load=None, after_load=None):
        if self.from_conn is not None:
            self.logger.debug("Creating extract cursor")
            extract_cur = self.from_conn.cursor()

        if before_extract is not None:
            self.logger.debug("Running before_extract")
            before_extract(self.from_conn.cursor())

        if self.extract_query is not None:
            self.logger.debug("Execute extract_query")
            extract_cur.execute(self.extract_query[0], self.extract_query[1])

        if after_extract is not None:
            self.logger.debug("Running after_extract")
            after_extract(extract_cur)

        if self.transform_function is not None:
            self.logger.debug("Transforming Data")
            transformed_data = []
            for i, row in enumerate(extract_cur):
                self.logger.debug("Transforming row: {}".format(str(i)))
                transformed_data.append(self.transform_function(row))

        if self.to_conn is not None:
            self.logger.debug("Creating load cursor")
            load_cur = self.to_conn.cursor()

        if before_load is not None:
            self.logger.debug("Running before_load")
            before_load(self.to_conn.cursor(), transformed_data)

        if self.load_query is not None:
            self.logger.debug("Loading Data")
            for i, data in enumerate(transformed_data):
                self.logger.debug("Transforming row: {}".format(str(i)))
                load_cur.execute(self.load_query[0], data)

        if after_load is not None:
            self.logger.debug("Running after_load")
            after_load(load_cur)

        self.logger.debug("Commiting")
        self.to_conn.commit()
        extract_cur.close()
        load_cur.close()
        self.logger.debug("Done execute")

        return self

