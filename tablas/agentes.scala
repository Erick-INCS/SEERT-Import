val sql = """SELECT
    Ca_Agencia.AgA_RazonSocial,
    Ca_Agencia.AgA_Calle AS Direccion,
    Ca_Agencia.AgA_Colonia,
    Ca_Agencia.AgA_Ciudad,
    Ca_Agencia.Pai_Clave,
    If(
        IsNull(Ca_AgenciaPatente.AgP_Rfc),
        If(
            IsNull(Ca_Agencia.AgA_Rfc_Irs)
            Or Ca_Agencia.AgA_Rfc_Irs = 'XEXX010101000',
            Ca_Agencia.AgA_Consecutivo,
            Ca_Agencia.AgA_Rfc_Irs
        ),
        Ca_AgenciaPatente.AgP_Rfc
    ) AS RFC,
    Ca_AgenciaPatente.AgP_Patente AS Patente
FROM
    Ca_AgenciaPatente
    RIGHT JOIN Ca_Agencia ON Ca_AgenciaPatente.AgA_Consecutivo = Ca_Agencia.AgA_Consecutivo;"""

registerDF(Connections.mssql, "Ca_AgenciaPatente")

val t = new Table(
	new SchemaTable("Ca_Agencia", Connections.mssql),
	new SchemaTable("AGENTESA"),
	Seq(
		(new Column("AgA_RazonSocial"), new Column("AG_NOMBRE")),
		(new Column("Direccion"), new Column("AG_DIRECCION")),
		(new Column("AgA_Colonia"), new Column("AG_COLONIA")),
		(new Column("AgA_Ciudad"), new Column("AG_CIUDAD")),
		(new Column("Pai_Clave"), new Column("AG_PAIS")),
		(new Column("RFC", false, true), new Column("AG_RFC")),
		(new Column("Patente"), new Column("AG_PATENTE"))
	),
	sql,
	Connections.fb
)

saveBatchTable("agentes.sql", 500, t)
