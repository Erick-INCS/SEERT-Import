
registerDFList(Seq(
  (Connections.fb, "FUMPRODUCTOS"),
  (Connections.mssql, "GR_PRODUCTOS_fix_NPARTE")
  )
)

saveBatch("fum_FraccMEX.sql",
  500,
  spark.sql("""SELECT
    GR_PRODUCTOS_fix_NPARTE.NParte,
    'FRACCMEX' AS Tipo,
    GR_PRODUCTOS_fix_NPARTE.UMedidaFraccion,
    GR_PRODUCTOS_fix_NPARTE.FactConv,
    GR_PRODUCTOS_fix_NPARTE.FracMex,
    'MEX' AS Clasificacion
  FROM
      GR_PRODUCTOS_fix_NPARTE
      LEFT JOIN FUMPRODUCTOS ON
          GR_PRODUCTOS_fix_NPARTE.NParte = FUMPRODUCTOS.FU_NPARTE AND
          FUMPRODUCTOS.FU_UMEDIDA = GR_PRODUCTOS_fix_NPARTE.UMedidaFraccion AND
          FUMPRODUCTOS.FU_TIPO = 'FRACCMEX'
  WHERE
        GR_PRODUCTOS_fix_NPARTE.FracMex IS NOT NULL AND
        FUMPRODUCTOS.FU_NPARTE IS NULL AND
        FUMPRODUCTOS.FU_TIPO IS NULL AND
        FUMPRODUCTOS.FU_UMEDIDA IS NULL;
  """).
    map(row => {
      s"""INSERT INTO
    FUMPRODUCTOS (
        FU_NPARTE,
        FU_TIPO,
        FU_UMEDIDA,
        FU_FACTORALT,
        FU_FRACCALT,
        FU_CLASIFICACION
    ) VALUES (
      ${if (row.getAs[String]("NParte") == null) "NULL" else s"'${row.getAs[String]("NParte").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("Tipo") == null) "NULL" else s"'${row.getAs[String]("Tipo").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("UMedidaFraccion") == null) "NULL" else s"'${row.getAs[String]("UMedidaFraccion").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n").toUpperCase}'"},
      ${if (row.getAs[String]("FactConv") == null) "NULL" else s"'${row.getAs[String]("FactConv")}'"},
      ${if (row.getAs[String]("FracMex") == null) "NULL" else s"'${row.getAs[String]("FracMex").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("Clasificacion") == null) "NULL" else s"'${row.getAs[String]("Clasificacion").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"}
    );\n"""
    }))
