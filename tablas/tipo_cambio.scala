val t = new Table(
	new SchemaTable("Ca_TipoCambio", Connections.mssql),
	new SchemaTable("TIPOCAMBIO"),
	Seq(
		(new Column("TiC_Fecha"), new Column("TC_FECHA")),
		(new Column("TiC_Valor"), new Column("TC_TCAMBIO"))
	),
	null,
	Connections.fb
)

saveBatchTable("TipoCambio.sql", 500, t)
