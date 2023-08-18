import pandas as pd
from functions import DataframeToMongo
import pdb

DM = DataframeToMongo()

dfCar = pd.DataFrame({
        'Carro': ['Onix', 'Polo', 'Sandero', 'Fiesta', 'City'],
        'Cor': ['Prata', 'Branco', 'Prata', 'Vermelho', 'Preto'],
        'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda']
        })

dfMont = pd.DataFrame({
    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda'],
    'País': ['EUA', 'Alemanhã', 'França', 'EUA', 'Japão']
})


pipeline = [
    {
        "$lookup": {
            "from": "Montadoras",
            "localField": "Montadora",
            "foreignField": "Montadora",
            "as": "montadora_info"
        }
    },
    {
        "$unwind": "$montadora_info"
    },
    {
        "$project": {
            "_id": 0,
            "Carro": 1,
            "Cor": 1,
            "Montadora": 1,
            "País": "$montadora_info.País"
        }
    },
    {
        "$group": {
            "_id": "$País",
            "Carros": {
                "$push": {
                    "Carro": "$Carro",
                    "Cor": "$Cor",
                    "Montadora": "$Montadora"
                }
            }
        }
    }
]

DM.insert_dataframes("Carros",dfCar)
DM.insert_dataframes("Montadoras",dfMont)
result = DM.agreg_pipe(pipeline,"Carros")
pd.DataFrame(result).to_json('result.json')




