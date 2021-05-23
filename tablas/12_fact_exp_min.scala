val t = new Table(
	new SchemaTable("De_Factura", Connections.mssql),
	new SchemaTable("FACTEXPORT"),
	Seq(
		(new Column("FEx_Folio", false, true, 30), new Column("EX_NFACTEXP")),
		(new Column("FEx_Fecha"), new Column("EX_FACTFECHA")),
		(new Column("FEx_TipoCambio", true), new Column("EX_TCAMBIO")),
		(new Column("ClP_Clave", false, false, 3), new Column("EX_CLVPEDIMENTO"))
	),
	null,
	Connections.fb
)

saveBatchTables(
	Seq("fact_exp.sql"),
	Seq(t)
)
