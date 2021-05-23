val t = new Table(
	new SchemaTable("Di_Factura", Connections.mssql),
	new SchemaTable("FACTIMPORT"),
	Seq(
		(new Column("FIm_Folio", false, true), new Column("IM_NFACTIMP")),
		(new Column("FIm_Fecha"), new Column("IM_FACTFECHA")),
		(new Column("FIm_TipoCambioUsd", true), new Column("IM_TCAMBIO")),
		(new Column("ClP_Clave"), new Column("IM_CLVOPERACION"))
	),
	null,
	Connections.fb
)

saveBatchTables(
	Seq("fact_imp.sql"),
	Seq(t)
)
