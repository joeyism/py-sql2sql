import logging

class ETL(object):
    from_conn = None
    from_initial_query = None
    from_final_query = None

    to_conn = None
    to_initial_query = None
    to_final_query = None

    extract_query = None
    transform_function = None
    load_query = None

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

    def from_conn(self, conn, initial_query=None, final_query=None):
        """
        source connection

        Parameters
        ----------
        conn:
            input connection
        """
        self.from_conn = conn
        self.from_initial_query = initial_query
        self.from_final_query = final_query

        return self

    def to_conn(self, conn, initial_query=None, final_query=None):

        """
        destination connection

        Parameters
        ----------
        conn:
            output connection
        """

        self.to_conn = conn
        self.to_initial_query = initial_query
        self.to_final_query = final_query
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

    def execute(self, batch_size = 1000):
        """
        Executes the ETL in the order of

        * from_conn
        * from_initial_query
        * to_conn
        * to_initial_query
        * extract
        * transform and load
        * from_final_query
        * to_final_query


        Parameters
        ----------
        batch_size: int
            batch size of the transform and load portion
        """
        if self.from_conn is not None:
            self.logger.debug("Creating extract cursor")
            extract_cur = self.from_conn.cursor()

        if self.from_initial_query is not None:
            from_initial_cur = self.from_conn.cursor()
            from_initial_cur.execute(self.from_initial_query)
            from_initial_cur.close()

        if self.to_conn is not None:
            self.logger.debug("Creating load cursor")
            load_cur = self.to_conn.cursor()

        if self.to_initial_query is not None:
            to_initial_cur = self.to_conn.cursor()
            to_initial_cur.execute(self.to_initial_query)
            to_initial_cur.close()


        # Extract
        self.logger.debug("Execute extract_query")
        extract_cur.execute(self.extract_query[0], self.extract_query[1])

        # Transform and Load
        i = 1
        while True:
            fetched_data = extract_cur.fetchmany(batch_size)
            if len(fetched_data) == 0:
                break

            self.logger.debug("Transforming row: {}".format(str(i*batch_size)))
            transformed_data = []
            for row in fetched_data:
                transformed_data.append(self.transform_function(row))

            self.logger.debug("Loading row: {}".format(str(i*batch_size)))
            for row in transformed_data:
                load_cur.execute(self.load_query[0], row)

            i += 1


        if self.from_final_query is not None:
            from_final_cur = self.from_conn.cursor()
            from_final_cur.execute(self.from_final_query)
            from_final_cur.close()

        if self.to_final_query is not None:
            to_final_cur = self.to_conn.cursor()
            to_final_cur.execute(self.to_final_query)
            to_final_cur.close()

        self.logger.debug("Commiting")
        self.to_conn.commit()
        extract_cur.close()
        load_cur.close()
        self.logger.debug("Done execute")

        return self

