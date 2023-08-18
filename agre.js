db.Carros.aggregate([
    {
        $lookup: {
            from: "Montadoras", 
            localField: "Montadora",
            foreignField: "Montadora",
            as: "montadora_info"
        }
    },
    {
        $unwind: "$montadora_info"
    },
    {
        $project: {
            _id: 0,
            Carro: 1,
            Cor: 1,
            Montadora: 1,
            País: "$montadora_info.País"
        }
    },
    {
        $group: {
            _id: "$País",
            Carros: {
                $push: {
                    Carro: "$Carro",
                    Cor: "$Cor",
                    Montadora: "$Montadora"
                }
            }
        }
    }
]);
