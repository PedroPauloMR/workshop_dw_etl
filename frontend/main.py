import streamlit as st 
from aws.client import S3Client
from datasource.csv import CSVCollector 
from contracts.catalogo import Catalogo

st.title('Essa é uma página de portal de dados')

aws_instancia = S3Client()
catalogo_de_produto = CSVCollector(Catalogo, aws_instancia, "C11:I211")
catalogo_de_produto.start()