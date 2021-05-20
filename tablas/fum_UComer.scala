
registerDFList(Seq(
  // (Connections.fb, "FUMPRODUCTOS"),
  (Connections.mssql, "GR_PRODUCTOS_fix_NPARTE")
  )
)

saveBatch("fum_ucomer.sql",
  500,
  spark.sql("""SELECT
    GR_PRODUCTOS_fix_NPARTE.NParte,
    'UMCOMER' AS Tipo,
    GR_PRODUCTOS_fix_NPARTE.UMComercial,
    GR_PRODUCTOS_fix_NPARTE.FactConvUComer
  FROM
      GR_PRODUCTOS_fix_NPARTE
  WHERE
        GR_PRODUCTOS_fix_NPARTE.UMComercial IS NOT NULL
  """).
    map(row => {
      s"""UPDATE OR INSERT INTO
    FUMPRODUCTOS (
        FU_NPARTE, FU_TIPO, FU_UMEDIDA, FU_FACTORALT
    ) VALUES (
      ${if (row.getAs[String]("NParte") == null) "NULL" else s"'${row.getAs[String]("NParte").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("Tipo") == null) "NULL" else s"'${row.getAs[String]("Tipo").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("UMComercial") == null) "NULL" else s"'${row.getAs[String]("UMComercial").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n").toUpperCase}'"},
      ${if (row.getAs[String]("FactConvUComer") == null) "NULL" else s"'${row.getAs[String]("FactConvUComer")}'"}
    ) MATCHING (FU_NPARTE, FU_TIPO, FU_UMEDIDA);\n"""
    }))
