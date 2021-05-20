
registerDFList(Seq(
  // (Connections.fb, "FUMPRODUCTOS"),
  (Connections.mssql, "GR_PRODUCTOS_fix_NPARTE")
  )
)

saveBatch("fum_fraccUSA.sql",
  500,
  spark.sql("""SELECT
    GR_PRODUCTOS_fix_NPARTE.NParte,
    'FRACCUSA' AS Tipo,
    GR_PRODUCTOS_fix_NPARTE.UMedidaFraccion,
    GR_PRODUCTOS_fix_NPARTE.ArancelUs,
    'USA' AS Clasificacion
  FROM
      GR_PRODUCTOS_fix_NPARTE
  WHERE
        GR_PRODUCTOS_fix_NPARTE.ArancelUs IS NOT NULL
  """).
    map(row => {
      s"""UPDATE OR INSERT INTO
    FUMPRODUCTOS (
        FU_NPARTE, FU_TIPO, FU_UMEDIDA, FU_FRACCALT, FU_CLASIFICACION
    ) VALUES (
      ${if (row.getAs[String]("NParte") == null) "NULL" else s"'${row.getAs[String]("NParte").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("Tipo") == null) "NULL" else s"'${row.getAs[String]("Tipo").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("UMedidaFraccion") == null) "NULL" else s"'${row.getAs[String]("UMedidaFraccion").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n").toUpperCase}'"},
      ${if (row.getAs[String]("ArancelUs") == null) "NULL" else s"'${row.getAs[String]("ArancelUs").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"},
      ${if (row.getAs[String]("Clasificacion") == null) "NULL" else s"'${row.getAs[String]("Clasificacion").replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"}
    ) MATCHING (FU_NPARTE, FU_TIPO, FU_UMEDIDA);\n"""
    }))


// SELECT
//     GR_PRODUCTOS_fix_NPARTE.NParte,
//     'FRACCUSA' AS Tipo,
//     GR_PRODUCTOS_fix_NPARTE.UMedidaFraccion,
//     GR_PRODUCTOS_fix_NPARTE.ArancelUs,
//     'USA' AS Clasificacion
//   FROM
//       GR_PRODUCTOS_fix_NPARTE
//       LEFT JOIN FUMPRODUCTOS ON
//           GR_PRODUCTOS_fix_NPARTE.NParte = FUMPRODUCTOS.FU_NPARTE AND
//           FUMPRODUCTOS.FU_TIPO = 'FRACCUSA'
//   WHERE
//         GR_PRODUCTOS_fix_NPARTE.ArancelUs IS NOT NULL AND
//         FUMPRODUCTOS.FU_NPARTE IS NULL AND
//         FUMPRODUCTOS.FU_TIPO IS NULL AND
//         FUMPRODUCTOS.FU_UMEDIDA IS NULL;