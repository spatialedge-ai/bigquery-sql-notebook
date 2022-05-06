# Utility
import hashlib
import json
import os
import pandas as pd
import mesh

from datetime import datetime
from mesh.google.connectors import bigquery


class BqState:

    def __init__(self, project_name, service_key_filename, project_folder='.'):
        self.query_registry = {}
        self.query_reg = []
        self.history = self.load_history(project_folder)
        self.data_registry = {}
        self.registered_queries = self.load_query_registry()
        self.conn = bigquery.BqConnector(project_name, service_key_filename=service_key_filename)

    def load_history(self, project_folder='.'):
        """
        :param project_folder: a folder in which to store history and queries
        Load or create usage history
        """
        if os.path.isfile(f"{project_folder}/.history"):
            with open(f"{project_folder}/.history") as history_file:
                return json.loads(history_file.read())
        else:
            return {
                "queries": [],
                "total_usage": 0
            }

    def load_query_registry(self):
        """
        Load registered queries
        """

        return self.query_reg

    def register_query(self, query, identifier=None):
        """
        Register a query using provided identifier

        :param identifier: Identifier for the query
        :param query: The registered query
        """
        if not identifier:
            #             import randomname
            #             identifier = randomname.get_name()
            import random
            import string
            identifier = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        self.query_registry[identifier] = query
        self.query_reg.append({
            "identifier": identifier,
            "query": query,
        })
        return identifier

    def _add_usage(self, identifier, query, usage):
        """
        Update usage totals for an executed query

        :param query: The executed query
        :param usage: Usage of executed query in bytes
        """

        self.history["queries"].append({
            "timestamp": datetime.now(),
            "identifier": identifier,
            "query": query,
            "usage": usage
        })

        self.history["total_usage"] += usage

        with open(".history", "w") as history_file:
            history_file.write(json.dumps(self.history, default=str))

    def _load(self, identifier):
        """
        Load data for query with specified identifier. Load data from cache is it exists.

        :param identifier: The identifier of the query to load
        """

        query = self.query_registry[identifier]
        query_checksum = hashlib.md5(query.encode("utf-8")).hexdigest()

        filename = f"{identifier}-{query_checksum}.csv"

        df = None
        if os.path.isfile(filename):
            print(f"Loading {identifier} from disk")
            df = pd.read_csv(filename)
        else:
            df = self.execute(identifier)

        return df

    def execute(self, identifier):
        """
        Load query with specified identifier from gcp

        :param identifier: The identifier of the query to load
        """

        query = self.query_registry[identifier]
        calc = self.conn.dry_run(query) / 1024 ** 4 * 5 * 14.6
        print(f"Query will cost approximately R{calc}. Would you like to continue? [Y]n")
        choice = input()
        if choice == "" or choice.lower() == "y":
            df = self.conn.read_pandas_dataframe(query)
            self._add_usage(identifier, query, calc)
            print(f"Total cost: R{self.history['total_usage']}")
            return df

