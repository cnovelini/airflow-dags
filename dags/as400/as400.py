import pandas
import pyodbc
from as400.constants import COMMIT_MODE_CS, CONNECTION_TYPE_READWRITE
from airflow.models import Variable



class AS400Connector:
    def __init__(self):
        self.host = Variable.get("coh_hostname")
        self.username = Variable.get("coh_username")
        self.password = Variable.get("coh_password")

    def get_connection(self):
        print("Connectingt to host {} with username {}....".format(self.host, self.username))

        return pyodbc.connect(
            self._connstr(
                self.host,
                self.username,
                self.password,
                COMMIT_MODE_CS,
                CONNECTION_TYPE_READWRITE,
            )
        )

    def _connstr(self, system, username, password, commitmode=None, connectiontype=None):
        connstr = "DRIVER=IBM i Access ODBC Driver;SYSTEM={};UID={};PWD={};SIGNON=1;TRANSLATE=1;".format(system, username, password)
        if commitmode is not None:
            connstr = connstr + f"CommitMode={commitmode};"
        if connectiontype is not None:
            connstr = connstr + f"ConnectionType={connectiontype};"

        return connstr
        
    def query_as_df(self, expression, user_identification):
        try:
            print(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Initializing as400 Query<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
            )

            connection = self.get_connection()
            result = pandas.read_sql(expression, connection)

            print(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>End as400 Query<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
            )
        except Exception as err:
            error_message = "*********************************Error during as400 Query*********************************"
            error_message += "\n"
            error_message += str(err)
            error_message += "\n"
            error_message += (
                "******************************************************************************************"
            )

            print(error_message)
            return False, {"error": f'Error during query execution" {err}'}
        return True, result
