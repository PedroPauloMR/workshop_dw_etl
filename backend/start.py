from datasource.api import APICollector
from contracts.schema import GenericSchema, CompraSchema
from tools.aws.client import S3Client
import schedule
import time

aws = S3Client()

def apiCollector(CompraSchema, aws, repeat):
    response = APICollector(CompraSchema, aws).start(repeat)
    print('Executei')
    return

schedule.every(1).minutes.do(apiCollector,CompraSchema, aws, 10)


while True:
    schedule.run_pending()
    time.sleep(2)