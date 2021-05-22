val t = new Table(
	new SchemaTable("GR_PEDIMENTO_EXP", Connections.mssql),
	new SchemaTable("PEDIMENTOEXP"),
	Seq(
		(new Column("Clave", false, true), new Column("PE_PEDIMENTOEXP")),
		(new Column("FechaIni"), new Column("PE_FECHAINICIAL")),
		(new Column("FechaFin"), new Column("PE_FECHAFINAL")),
		(new Column("Consolidado"), new Column("PE_CONSOLIDADO")),
		(new Column("ADV", true), new Column("PE_ADV")),
		(new Column("IVA", true), new Column("PE_IVA")),
		(new Column("Importe", true), new Column("PE_IMPORTE")),
		(new Column("Observaciones"), new Column("PE_OBSERVACIONES")),
		(new Column("TCambio", true), new Column("PE_TCAMBIO")),
		(new Column("ClvAduana"), new Column("PE_CLVADUANA")),
		(new Column("AgPatente"), new Column("PE_AGPATENTE")),
		(new Column("ClvPedimento"), new Column("PE_CLVPEDIMENTO")),
		(new Column("AgenteRFC"), new Column("PE_AGENTERFC")),
		(new Column("ValorAduana", true), new Column("PE_VALORADUANA")),
		(new Column("FechaPAgo"), new Column("PE_FECHAPAGO"))
	),
	null,
	Connections.fb
)

saveBatchTables(
	Seq("pedimentos_imp.sql"),
	Seq(t)
)
