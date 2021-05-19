val t = new Table(
	new SchemaTable("Ca_TipoCambio", Connections.mssql),
	new SchemaTable("TIPOCAMBIO"),
	Seq(
		(new Column("TiC_Fecha"), new Column("TC_FECHA")),
		(new Column("TiC_Valor"), new Column("TC_TCAMBIO"))
	)
)

saveBatchTable("TipoCambio.sql", 500, t)
