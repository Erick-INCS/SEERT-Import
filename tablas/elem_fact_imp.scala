val t = new Table(
	new SchemaTable("GR_ELEM_FACTIMPORT", Connections.mssql),
	new SchemaTable("ELEMFACTIMP"),
	Seq(
		(new Column("FIm_Folio", false, true), new Column("EI_NFACTIMP")),
		(new Column("FId_NoParte", false, true), new Column("EI_NPARTE")),
		(new Column("Pai_Clave", false, true), new Column("EI_PAISORIGEN")),
		(new Column("Tipo"), new Column("EI_NPTIPO")),
		(new Column("TipoParte"), new Column("EI_TIPOPARTE")),
		(new Column("FId_CostoUnit", true), new Column("EI_VALORUNIT")),
		(new Column("FId_DescripcionEsp", false, false, 150), new Column("EI_DESCESP")),
		(new Column("FId_DescripcionIng", false, false, 150), new Column("EI_DESCING")),
		(new Column("Med_Clave"), new Column("EI_UMPARTE")),
		(new Column("Med_Fraccion"), new Column("EI_UMFRACCION")),
		(new Column("Pid_FactConvMx", true), new Column("EI_FCONVERSION")),
		(new Column("FRACCUSA"), new Column("EI_ARANCELUS")),
		(new Column("FRACCMEX"), new Column("EI_ARANCELMX")),
		(new Column("PaisProcedencia"), new Column("EI_PAISPROCEDENCIA")),
		(new Column("FId_Cantidad", true), new Column("EI_CANTMATERIAL")),
		(new Column("Psa_Saldo", true), new Column("EI_BALANCE")),
		(new Column("FId_PesoNeto", true), new Column("EI_PESONETO")),
		(new Column("FId_PesoBru", true), new Column("EI_PESOBRUTO")),
		(new Column("FactFecha"), new Column("EI_FACTFECHA")),
		(new Column("NPedimentoGr"), new Column("EI_PEDIMENTOIMP")),
		(new Column("Pim_FechaPago"), new Column("EI_FECHAPED")),
		(new Column("Pid_FechaVencimiento31"), new Column("EI_FECHAVENCE")),
	),
	null,
	Connections.fb
)

saveBatchTables(
	Seq("elem_fact_imp.sql"),
	Seq(t)
)