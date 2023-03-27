from new.database_utils import connector
import pandas as pd
import tabula
import requests
import json
import boto3 

#s3_client = boto3.client('s3')

class DataExtractor():
    #def __init__(self) -> None:
        #self.conn = connector.read_db_credsdata()
        # why not start with init_db method

    def read_rds_table(self, table_name, engine):
        credentials = connector.read_db_credsdata()
        engine = connector.init_db_engine(credentials)
        table_name = connector.list_db_tables(engine)
        #print(table_name)
        data = pd.read_sql_table(table_name[1], engine)
        #print(data.columns)
        return data
     
    
    def retrieve_pdf_data(self, link):
        pdf= tabula.read_pdf(link,  pages='all')
        #print(len(pdf))
        df_pdf = pd.concat(pdf)
        #print(df_pdf.shape)
        #print(df_pdf.head())
        return df_pdf

    def list_number_of_stores (self, endpoint, headers):
        response = requests.get(endpoint, headers = headers)
        #headers = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        content = response.text
        result = json.loads(content)
        store_number = list(result.values())[1]
        #print(store_number)
        return store_number
 
    def retrieve_stores_data(self):
        data = []
        
        for i in range(0, store_number):
            store_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
            response = requests.get(store_endpoint, headers = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}).text
            response_dict = json.loads(response)
            #print(type(response_dict))
            data.append(response_dict) 
        data = pd.DataFrame(data)
        #print(data.head())
        #print(data.head())

            #print(response.content)
        return data
        #headers = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        # store_data = pd.Dataframe(store_data_api)
        # print(store_data_api.status_code)
        # return store_data_api


    def extract_from_s3(self):
        # s3 = boto3.resource('s3')
        # my_bucket = s3.Bucket('data-handling-public')
        # for file in my_bucket.objects.all():
        #     print(file.key)
        s3_client = boto3.client('s3')
        # s3_client.Bucket('data-handling-public.s3.eu-west-1.amazonaws.com').download_file(Key='date_details.json', Filename='s3_data.json')
        # s3_data = pd.read_json('s3_data.json')#s3_client.download_file(Bucket = 'data-handling-public', Key = 'products.csv', Filename = 's3_data.csv'  )
        #return s3_data
        result = s3_client.get_object(Bucket = 'data-handling-public', Key= 'date_details.json' )
        json_data = result['Body'].read()
        df_json = pd.read_json(json_data)
        return df_json
        




        
        


#if __name__ == "__main__":
data_extractor = DataExtractor()
#credentials = connector.read_db_credsdata()
# credentials = connector.read_db_credsdata()
# engine = connector.init_db_engine(credentials)
# table_name = connector.list_db_tables(engine)
# data_extractor.read_rds_table(table_name[2], engine)
store_number = data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',{'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'} )
#data_extractor.retrieve_stores_data()
#data_extractor.extract_from_s3()
       