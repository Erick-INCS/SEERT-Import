val t = new Table(
	new SchemaTable("GR_ELEM_FACTIMPORT", Connections.mssql),
	new SchemaTable("PEDIMENTOIMP", Connections.fb),
	Seq(
		(new Column("NPedimentoGr", false, true), new Column("PI_PEDIMENTOIMP")),
		(new Column("Fecha", false, true), new Column("PI_FCADUCIDAD"))
	),
	"select NPedimentoGr, MAX(Pid_FechaVencimiento31) AS Fecha from GR_ELEM_FACTIMPORT LEFT JOIN PEDIMENTOIMP ON PEDIMENTOIMP.PI_PEDIMENTOIMP =  NPedimentoGr  WHERE  PEDIMENTOIMP.PI_PEDIMENTOIMP IS NULL group by NPedimentoGr;",
	Connections.fb
)

saveBatchTables(
	Seq("pedimentos_imp.sql"),
	Seq(t)
)
