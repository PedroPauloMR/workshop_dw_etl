from fastapi import FastAPI
from faker import Faker
import pandas as pd
import random


app = FastAPI()
fake = Faker()

file_name = 'backend/fakeapi/products.csv'
df = pd.read_csv(file_name)
df['indice'] = range(1, len(df) + 1)
df.set_index('indice', inplace = True)


@app.get('/gerar_compra')
async def gerar_compra():
    index = random.randint(1, len(df) - 1)
    tupla = df.iloc[index]
    return [{
        'client': fake.name(),
        'creditcard': fake.credit_card_provider(),
        'product_name':tupla['Product Name'],
        'ean': int(tupla['EAN']),
        'price': round(float(tupla['Price']*1.2),2),
        'store': 11,
        'dateTime': fake.iso8601(),
        'clientPosition': fake.location_on_land()
    }]
     


@app.get('/gerar_compras/{numero_registro}')
async def gerar_compras(numero_registro: int):
    if numero_registro < 1:
        return {'error': 'O número deve ser maior que 1'}
    
    respostas = []
    for _ in range(numero_registro):
        index = random.randint(1, len(df) - 1)
        tupla = df.iloc[index]
        pessoa = {
            'client': fake.name(),
            'creditcard': fake.credit_card_provider(),
            'product_name':tupla['Product Name'],
            'ean': int(tupla['EAN']),
            'price': round(float(tupla['Price']*1.2),2),
            'store': 11,
            'dateTime': fake.iso8601(),
            'clientPosition': fake.location_on_land()
        }
        respostas.append(pessoa)
    return respostas

