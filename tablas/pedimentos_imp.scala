val t = new Table(
	new SchemaTable("GR_PEDIMENTO_IMP", Connections.mssql),
	new SchemaTable("PEDIMENTOIMP"),
	Seq(
		(new Column("Clave"), new Column("PI_PEDIMENTOIMP")),
		(new Column("Ini"), new Column("PI_FECHAINICIAL")),
		(new Column("FechaCaducidad"), new Column("PI_FCADUCIDAD")),
		(new Column("FechaFinal"), new Column("PI_FECHAFINAL")),
		(new Column("TCambio"), new Column("PI_TCAMBIO")),
		(new Column("Importe"), new Column("PI_IMPORTE")),
		(new Column("ADV"), new Column("PI_ADV")),
		(new Column("IVA"), new Column("PI_IVA")),
		(new Column("ClvAduana"), new Column("PI_CLVADUANA")),
		(new Column("AgPatente"), new Column("PI_AGPATENTE")),
		(new Column("Status"), new Column("PI_STATUS")),
		(new Column("Notas"), new Column("PI_NOTAS")),
		(new Column("ClvPedimento"), new Column("PI_CLVPEDIMENTO")),
		(new Column("ImpIgiPagado"), new Column("PI_IMPIGIPAGADO")),
		(new Column("AgenteRFC"), new Column("PI_AGENTERFC")),
		(new Column("ValorAduana"), new Column("PI_VALORADUANA")),
		(new Column("FechaPAgo"), new Column("PI_FECHAPAGO"))
	)
)

saveBatchTable("pedimentos_imp.sql", 500, t)
