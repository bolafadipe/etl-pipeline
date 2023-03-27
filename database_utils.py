import yaml
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import inspect

class DatabaseConnector():
    
    def read_db_credsdata(self):
        with open("C:\AI\Data Project\multinational-retail-data-centralisation\db_creds.yaml") as f:
            credentials = yaml.load(f, Loader=yaml.FullLoader)
            #print(credentials)
            return credentials
            
    def init_db_engine(self, credentials):

        engine = create_engine(f"{credentials['RDS_DATABASE_TYPE']}+{credentials['DBAPI']}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['DATABASE']}")
        engine.connect()
        return engine
    
    def list_db_tables(self, engine):
        #engine = self.init_db_engine(credentials)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        print(table_names)
        return table_names
    
    # Now create a method in your DatabaseConnector class called upload_to_db. 
    # This method will take in a Pandas DataFrame and 
    # table name to upload to as an argument.
    def upload_to_db(self, df , table_name):
        credentials = self.read_db_credsdata()
        conn = create_engine(f"{credentials['Local_DATABASE_TYPE']}+{credentials['Local_DBAPI']}://{credentials['Local_USER']}:{credentials['Local_PASSWORD']}@{credentials['Local_HOST']}:{credentials['Local_PORT']}/{credentials['Local_DATABASE']}")
        conn.connect()
        df.to_sql(name = table_name, con = conn, if_exists = 'replace')
        #return conn
        
    #Database = 

   

connector = DatabaseConnector()
credentials = connector.read_db_credsdata()
engine = connector.init_db_engine(credentials)
table_name = connector.list_db_tables(engine)
# df = cleaned_data.clean_user_data()
# df = connector.upload_to_db(df, table_name[1])





# print(table_names)









