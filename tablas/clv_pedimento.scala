val t = new Table(
	new SchemaTable("CaF_ClavePed", Connections.mssql),
	new SchemaTable("CLVPEDIM"),
	Seq(
		(new Column("ClP_Clave"), new Column("CP_CLVPED")),
		(new Column("ClP_Descripcion"), new Column("CP_DESCRIPCION"))
	)
)

saveBatchTable("ClavePed.sql", 500, t)
