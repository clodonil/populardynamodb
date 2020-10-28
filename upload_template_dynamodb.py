'''
file: save_pipeline_in_dynamodb.py
descricao: Script para salvar a estrutura da Pipeline no dynamodb, substituindo o lambda durante
           o desenvolvimento.
autor: Clodonil Honorio Trigo
email: clodonil@nisled.org
data: 27 de abril de 2019
'''

import boto3
import json
import glob
import sys



def save_dynamodb(dados):    
   resource_id = dados.pop('name')
   dynamodb  = boto3.resource('dynamodb')
   tablename = 'wasabi-template-produced'
   table = dynamodb.Table(tablename)
   print(f"--> Upload do template *{resource_id}* na tabela *{tablename}* do dynamodb", end =" ")
   table.update_item(Key={'name' :resource_id},
                      UpdateExpression="set detail = :a",
                      ExpressionAttributeValues={':a': dados['details']},      
                      ReturnValues="UPDATED_NEW"
                      )
   print('[ Salvo ]')

def listar_arquivos(path):
    files = glob.glob(path + '/*/templates.json')
    return files

def get_template_json(file):
    print("--> lendo o arquivo", file)
    with open(file, 'r') as f:
        dados = json.load(f)
    return dados


if __name__ == '__main__':    
    if len(sys.argv) > 1:
        print("Parametros passados:",str(sys.argv))
        path = sys.argv[1]
    else:
        print("Parametro do path dos templates nao informado")    
        sys.exit(1)
    files = listar_arquivos(path)    
    for file in files:
        dados = get_template_json(file)
        save_dynamodb(dados)

#    update(table,id,pipeline)
#    print(json.dumps(search(id), indent=4, sort_keys=True))