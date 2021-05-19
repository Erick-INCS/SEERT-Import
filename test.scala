val t = new Table(
	new SchemaTable("TESTTABLE", Connections.test),
	new SchemaTable("RES_TABLE"),
	Seq(
		(new Column("ID", true), new Column("_id")),
		(new Column("HASH"), new Column("code"))
	)
)

saveBatchTable("test.sql", 2, t)
