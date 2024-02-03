from io import BytesIO
import streamlit as st 
import openpyxl 
import pandas as pd
import datetime
from pydantic import ValidationError


class CSVCollector:
    def __init__(self, schema, aws, cell_range):
        self._schema = schema
        self._aws = aws
        self._buffer = None
        self.cell_range = cell_range
        return
    
    def start(self):
        getData = self.getData()
        extractData = None
        validateData = None
        if getData is not None:
            extractData = self.extractData(getData)
        
        if extractData is not None:
            validateData = self.validateData(extractData)
        
        if validateData is not None:
            
            df = self.convertToParquet(validateData)
            if self._buffer is not None:
                file_name = self.fileName()
                print(file_name)
                self._aws.upload_file(df, file_name)
                return True
        return False
    
    
    def getData(self):
        dados_excel = st.file_uploader('Insira o arquivo Excel', type = '.xlsx')
        return dados_excel
    
    
    def extractData(self, dados_excel):
        workbook = openpyxl.load_workbook(dados_excel)
        sheet = workbook.active
        range_cell = sheet[self.cell_range]

        headers = [cell.value for cell in range_cell[0]]

        data = []
        for row in range_cell[1:]:
            data.append([cell.value for cell in row])

        dataframe = pd.DataFrame(data, columns = headers)
        return dataframe
    
    
    def validateData(self, dataframe):
        error = []
        valid_rows = []

        for index, row in dataframe.iterrows():
            try:
                valid_row = self._schema(**row.to_dict())
                valid_rows.append(valid_row)
            except ValidationError as e:
                error.append(f'Erro na linha {index + 1}: {str(e)}')

        if error:
            st.error('\n'.join(error))
            return None
        
        st.success('Tudo certo !')
        return dataframe
    
    
    def convertToParquet(self, dataframe):
        self._buffer = BytesIO()
        try:
            dataframe.to_parquet(self._buffer)
            return self._buffer
        except:
            print('Erro ao transformar o DF em parquet')
            self.buffer = None
    
    def fileName(self):
        data_atual = datetime.datetime.now().isoformat()
        match = data_atual.split('.')
        return f"catalogo/catalogo.parquet" #f"catalogo/catalogo{match[0]}.parquet"