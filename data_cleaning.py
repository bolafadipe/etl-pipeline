from new.data_extraction import data_extractor
from new.database_utils import connector




class DataCleaning():
    def clean_user_data(self):
        credentials = connector.read_db_credsdata()
        engine = connector.init_db_engine(credentials)
        table_name = connector.list_db_tables(engine)
        df = data_extractor.read_rds_table(table_name, engine)
        error_list = ['I7G4DMDZOZ', 'NULL',
       'AJ1ENKS3QL', 'XGI7FM0VBJ', 'S0E37H52ON', 'XN9NGL5C0B',
       '50KUU3PQUF', 'EWE3U0DZIV', 'GMRBOMI0O1', 'YOTSVPRBQ7',
       '5EFAFD0JLI', 'PNRMPSYR1J', 'RQRB7RMTAD', '3518UD5CE8',
       '7ZNO5EBALT', 'T4WBZSW0XI']
        df = df[~(df.country.isin(error_list))]   
        # print(df.info())
        # date_format = "%Y-%m-%d"
        # pd.to_datetime(["date_of_birth"], format=date_format)
        df.drop_duplicates(inplace = True)
        df = df[~(df.company== 'NULL')]
        #print(df.info)
        
        return df
    

    def clean_card_data(self):
        df_pdf = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        #print(df_pdf.describe())
        #print(df_pdf.info())
        df_pdf = df_pdf[~(df_pdf.expiry_date== 'NULL')]
        error_list = ['NB71VBAHJE',
       'WJVMUO4QX6', 'JRPRLPIBZ2', 'TS8A81WFXV', 'JCQMU8FN85',
       '5CJH7ABGDR', 'DE488ORDXY', 'OGJTXI6X1H', '1M38DYQTZV',
       'DLWF2HANZF', 'XGZBYBYGUW', 'UA07L7EILH', 'BU9U947ZGV',
       '5MFWFBZRM9']
        df_pdf = df_pdf[~(df_pdf.card_provider.isin(error_list))]
        return df_pdf


    def called_clean_store_data(self):
        store_data = data_extractor.retrieve_stores_data()
        #print(data.info())
        store_data.drop(columns = 'lat', inplace = True)
        store_data.isna().sum() 
        store_data.dropna(axis = 0, how='any', inplace = True)
        store_data = store_data[~(store_data.address=='NULL')]
        error_list = ['BIP8K8JJW2', 'SKBXAXF5G5', '2429OB3LMM', '74BY7HSB6P', 'GT1FO6YGD4','FRTGHAA34B','D23PCWSM6S' ,'3n9', 'A97'
                        '80R','30e','J78']
        store_data = store_data[~(store_data.staff_numbers.isin(error_list))]  
        store_data['staff_numbers'] = store_data.staff_numbers.str.replace('A', '')
        store_data['staff_numbers'] = store_data.staff_numbers.str.replace('R', '')
        return store_data

    def convert_product_weights(self, data):
        
        data.weight.unique()
        data.isna().sum()
        data.dropna(axis = 0, how = 'any', inplace = True)
        data['weight'].mask(data.weight.str.contains('ml'), data.weight.str.replace('ml', 'g'), inplace = True)
        data['weight'] = data.weight.str.replace('kg' , '')
        data.loc[data.weight.str.contains('x')].weight.str.split('x', expand = True)
        data['weight'] = data.weight.str.split('x', expand = True)
        data['weight'].mask(data.weight.str.contains('oz'), data.weight.str.replace('oz', ''), inplace = True)
        data['weight'].mask(data.weight.str.contains('g'), data.weight.str.replace('g', ''), inplace = True)
        data.drop(index = [1400,1133,751], inplace = True)
        data['weight'] = data.weight.str.strip('.')
        data['weight']= data.weight.astype('float64')
        print(data.shape)
        return data

    def clean_orders_data(self):
        credentials = connector.read_db_credsdata()
        engine = connector.init_db_engine(credentials)
        table_name = connector.list_db_tables(engine)
        orders_data = data_extractor.read_rds_table(table_name[2], engine)
        orders_data.drop(columns=['first_name', 'last_name', '1', 'level_0'],inplace=True)
        print(orders_data.columns)
        return orders_data

    def clean_date_data(self):
        json_df = data_extractor.extract_from_s3()
        json_df.isna().sum()
        json_df = json_df[~(json_df.month=='NULL')]
        json_df.month.unique()
        error_list = ['1YMRDJNU2T',  '9GN4VIO5A8', 'NF46JOZMTA', 'LZLLPZ0ZUA',
       'YULO5U0ZAM', 'SAT4V9O2DL', '3ZZ5UCZR5D', 'DGQAH7M1HQ',
       '4FHLELF101', '22JSMNGJCU', 'EB8VJHYZLE', '2VZEREEIKB',
       'K9ZN06ZS1X', '9P3C0WBWTU', 'W6FT760O2B', 'DOIR43VTCM',
       'FA8KD82QH3', '03T414PVFI', 'FNPZFYI489', '67RMH5U2R6',
       'J9VQLERJQO', 'ZRH2YT3FR8', 'GYSATSCN88']

        json_df = json_df.loc[~(json_df.month.isin(error_list))]
        return json_df
        


cleaned_data = DataCleaning()
# cleaned_data.clean_user_data()
# df = cleaned_data.clean_user_data()
# connector.upload_to_db(df, 'dim_users')
cleaned_data.clean_card_data()
df_pdf = cleaned_data.clean_card_data()
connector.upload_to_db(df_pdf, 'dim_card_details')
# cleaned_data.called_clean_store_data()
# data = cleaned_data.called_clean_store_data()
# connector.upload_to_db(data, 'dim_store_details')
#data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

# product_data = data_extractor.extract_from_s3()
# cleaned_data.convert_product_weights(product_data)
# connector.upload_to_db(product_data, 'dim_products')

# date_data = cleaned_data.clean_date_data()
# connector.upload_to_db(date_data, 'dim_date_times')





