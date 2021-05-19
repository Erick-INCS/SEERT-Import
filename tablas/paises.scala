
registerDFList(Seq(
  (Connections.mssql, "CaF_Pais")
  )
)

saveBatch("paises.sql",
  500,
  spark.sql("select * from CaF_Pais;").
    map(row => {
      println(row.getAs[String]("Pai_Consecutivo").getClass) 
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
      ${func(row)}
    ) MATCHING (P_NPARTE);\n"""
    })
)

saveBatch("paises.sql",
  500,
  spark.sql("select * from CaF_Pais;").
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
      ${getValueOf(row, "Pai_Consecutivo")}
    ) MATCHING (P_NPARTE);\n"""
    })
)

pai_consecutivo, pai_clave, pai_nombreesp, pai_nombreing, pai_clavepedfiii, pai_iso, pai_iata, tra_clave, pai_origsistema
saveBatch("paises.sql",
  500,
  spark.sql("select * from CaF_Pais;").
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
      ${getValueOf(row, "NParte")},
      ${getValueOf(row, "DescEsp")},
      ${getValueOf(row, "DescIng")},
      ${getValueOf(row, "Tipo")},
      ${getValueOf(row, "TipoParte")},
      ${getValueOf(row, "UM")},
      ${getValueOf(row, "UMedidaFraccion")},
      ${getValueOf(row, "FracMex")},
      ${getValueOf(row, "ArancelUs")},
      ${getValueOf(row, "FactConv")},
      ${getValueOf(row, "Nota")},
      ${getValueOf(row, "Desp")},
      ${getValueOf(row, "Merm")}
    ) MATCHING (P_NPARTE);\n"""
    })
)
