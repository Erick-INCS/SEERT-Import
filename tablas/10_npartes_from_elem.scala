val t = new Table(
	new SchemaTable("GR_ELEM_FACTIMPORT", Connections.mssql),
	new SchemaTable("PRODUCTOS"),
	Seq(
		(new Column("FId_NoParte", false, true), new Column("P_NPARTE")),
	),
	"select LEFT(FId_NoParte, 30) AS FId_NoParte from GR_ELEM_FACTIMPORT group by FId_NoParte;",
	Connections.fb
)

saveBatchTables(
	Seq("partes.sql"),
	Seq(t)
)
