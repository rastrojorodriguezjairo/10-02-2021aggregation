/*En este ejemplo filtramos por la cafeteria, ya que nos interesan las ventas de Fruta*/
db.ventas.aggregate(
    [
        {
            $match: { 
                $expr: {$eq:["$Categoria","Frutas"]}
            }
        },
        {
            $group:
            {
                _id: { 
                    año: {$year: "$Fecha_de_la_venta" },
                    producto: "$Articulo_vendido"
                },
                venta_total: { $sum: { $multiply: ["$Precio_unitario", "$Número_de_unidades"] } }
            }
        },
        {
            $project: {
                año: "$_id.año",
                producto: "$_id.producto",
                _id: 0,
                totalv: "$venta_total",
                IVA: { $multiply: ["$venta_total", 0.21] },
                totalvIVA: { $multiply: ["$venta_total", 1.21] },
                totalRedondeado: { $round: [{ $multiply: ["$venta_total", 1.21] }, 0] }
            }
        },
        {
            $sort: {
                año: 1, producto: 1
            }
        },
        {
            $match: {
                totalvIVA: { $gt: 50 }
            }
        }
    ]
).pretty()