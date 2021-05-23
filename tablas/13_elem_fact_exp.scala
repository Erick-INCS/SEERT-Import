// val t1 = new Table(
// 	new SchemaTable("GR_ELEM_FACTEXPORT", Connections.mssql),
// 	new SchemaTable("PEDIMENTOEXP", Connections.fb),
// 	Seq(
// 		(new Column("NPedimentoExpGr", false, true, 25), new Column("PE_PEDIMENTOEXP")),
// 		(new Column("Fecha"), new Column("PE_FECHAFINAL"))
// 	),
// 	"select NPedimentoExpGr, MAX(Pex_FechaPago) AS Fecha from GR_ELEM_FACTEXPORT LEFT JOIN PEDIMENTOEXP ON PEDIMENTOEXP.PE_PEDIMENTOEXP =  NPedimentoExpGr  WHERE  PEDIMENTOEXP.PE_PEDIMENTOEXP IS NULL group by NPedimentoExpGr;",
// 	Connections.fb
// )

val t2 = new Table(
	new SchemaTable("GR_ELEM_FACTEXPORT", Connections.mssql),
	new SchemaTable("ELEMFACTEXP"),
	Seq(
		(new Column("FEx_Folio", false, true, 30), new Column("EE_NFACTEXP")),
		(new Column("Fed_NoParte", false, true), new Column("EE_NPARTE")),
		(new Column("TipoMatExp", false, true), new Column("EE_TIPOMATEXP")),
		(new Column("NPTipo"), new Column("EE_NPTIPO")),
		(new Column("Fed_DescripcionEsp", false, false, 150), new Column("EE_DESCESP")),
		(new Column("Fed_DescripcionIng", false, false, 150), new Column("EE_DESCING")),
		(new Column("Fed_Cantidad"), new Column("EE_TOTALMAT")),
		(new Column("MATGRAB"), new Column("EE_MATDUTIABLE")),
		(new Column("LABOR", true), new Column("EE_VALDUTIABLE")),
		(new Column("Fed_PesoUnit"), new Column("EE_PESOUNIT")),
		(new Column("Fed_PesoBru"), new Column("EE_PESOBRUTO")),
		(new Column("Fed_PesoNeto"), new Column("EE_PESONETO")),
		(new Column("FRACUSA", false, false, 10), new Column("EE_ARANCELUS")),
		(new Column("FRACCMX", false, false, 10), new Column("EE_ARANCELMX")),
		(new Column("TiM_Clave", false, false, 5), new Column("EE_UMPARTE")),
		(new Column("Med_Clave", false, false, 5), new Column("EE_UMFRACCION")),
		(new Column("Pai_Destino", false, false, 3), new Column("EE_PAISDESTINO")),
		(new Column("NPedimentoExpGr", false, false, 25), new Column("EE_PEDIMENTOEXP")),
		(new Column("Pex_FechaPago"), new Column("EE_FECHAPED")),
		(new Column("Pai_Origen", false, false, 3), new Column("EE_PAISORIGEN"))
	),
	null,
	Connections.fb
)

saveBatchTables(
	Seq("elem_fact_exp.sql"),
	Seq(t2)
)

// saveBatchTables(
// 	Seq("pedi_exp_from_elem_fact.sql", "elem_fact_exp.sql"),
// 	Seq(t1, t2)
// )