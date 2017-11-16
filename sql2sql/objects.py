import logging

class ETL(object):
    from_conn = None
    to_conn = None
    extract_query = None
    transform_function = None
    load_query = None
    load_all_query = None

    logger = logging.getLogger("etl")

    def __init__(self, log_level=0):
        """
        Creates the ETL object

        Parameters
        ----------
        log_level: default = 0, optional
            Determines the log level. It is the same the different log levels in logging.
            i.e.
            ETL(log_level=logging.DEBUG)
        """
        self.logger.setLevel(log_level)

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def from_conn(self, conn):
        """
        source connection

        Parameters
        ----------
        conn:
            input connection
        """
        self.from_conn = conn
        return self

    def to_conn(self, conn):

        """
        destination connection

        Parameters
        ----------
        conn:
            output connection
        """

        self.to_conn = conn
        return self

    def extract(self, statement, tup=()):
        """
        Extract statement

        Parameters
        ----------
        statement: string_like
            statement to extract data, probably a SELECT statement of some sort
        tup: (), optional
            tuple that could be passed in to help statement
        """

        self.extract_query = (statement, tup)
        return self

    def transform(self, f):
        """
        Transforms the data

        Parameters
        ----------
        f: function_like
            A function that transform the data. The documentation of the function has to be:

            Parameters
            ----------
            row: tuple_like
                A tuple that represents one row of data. This function will be run individually on each row of data
        """

        self.transform_function = f
        return self

    def load(self, statement, tup=()):
        """
        Load statement

        Parameters
        ----------
        statement: string_like
            statement to load data, probably an INSERT statement of some sort
        tup: (), optional
            tuple that could be passed in to help statement

        """
        self.load_query = (statement, tup)
        return self

    def load_all(self, statment, tup):
        self.load_all_query = (statement, tup)
        return self

    def execute(self, before_extract=None, after_extract=None, before_load=None, after_load=None):
        """
        Executes the ETL in the order of

        * from_conn
        * before_extract
        * extract
        * after_extract
        * transform
        * to_conn
        * before_load
        * load
        * after_load


        Parameters
        ----------
        before_extract: function_like
            A function that executes before extraction. The documentation of the function has to be:
                Parameters
                ----------
                cur: cursor_like
                    A cursor to the source connection

        after_extract: function_like
            A function that executes after extraction. The documentation of the function has to be:
                Parameters
                ----------
                cur: cursor_like
                    A cursor to the source connection

        before_load: function_like
            A function that executes before load. The documentation of the function has to be:
                Parameters
                ----------
                cur: cursor_like
                    A cursor to the source connection

        after_load: function_like
            A function that executes after load. The documentation of the function has to be:
                Parameters
                ----------
                cur: cursor_like
                    A cursor to the source connection
        """
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
                if i%1000 == 0:
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
                if i%1000 == 0:
                    self.logger.debug("Loading row: {}".format(str(i)))
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

