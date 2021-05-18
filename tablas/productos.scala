
registerDFList(Seq(
  // (Connections.fb, "PRODUCTOS"),
  (Connections.mssql, "GR_PRODUCTOS_fix_NPARTE")
  )
)

saveBatch("productos.sql",
  500,
  spark.sql("select * from GR_PRODUCTOS_fix_NPARTE;").
    map(row => {
      s"""UPDATE OR INSERT INTO
    PRODUCTOS (
        P_NPARTE,
        P_DESCESP,
        P_DESCING,
        P_TIPO,
        P_TIPOPARTE,
        P_UMEDIDAPARTE,
        P_UMEDIDAFRACCION,
        P_ARANCELMX,
        P_ARANCELUS,
        P_FACTCONVERSION,
        P_NOTAS,
        P_GENDESP,
        P_GENMERMA
    ) VALUES (
      ${if (row.getAs[String]("NParte") == null) "NULL" else s"'${row.getAs[String]("NParte").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("DescEsp") == null) "NULL" else s"'${row.getAs[String]("DescEsp").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("DescIng") == null) "NULL" else s"'${row.getAs[String]("DescIng").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("Tipo") == null) "NULL" else s"'${row.getAs[String]("Tipo")}'"},
      ${if (row.getAs[String]("TipoParte") == null) "NULL" else s"'${row.getAs[String]("TipoParte")}'"},
      ${if (row.getAs[String]("UM") == null) "NULL" else s"'${row.getAs[String]("UM").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n").toUpperCase}'"},
      ${if (row.getAs[String]("UMedidaFraccion") == null) "NULL" else s"'${row.getAs[String]("UMedidaFraccion").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n").toUpperCase}'"},
      ${if (row.getAs[String]("FracMex") == null) "NULL" else s"'${row.getAs[String]("FracMex").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("ArancelUs") == null) "NULL" else s"'${row.getAs[String]("ArancelUs").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("FactConv") == null) "NULL" else s"'${row.getAs[String]("FactConv")}'"},
      ${if (row.getAs[String]("Nota") == null) "NULL" else s"'${row.getAs[String]("Nota").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("Desp") == null) "NULL" else s"'${row.getAs[String]("Desp")}'"},
      ${if (row.getAs[String]("Merm") == null) "NULL" else s"'${row.getAs[String]("Merm")}'"}
    ) MATCHING (P_NPARTE);\n"""
    }))
