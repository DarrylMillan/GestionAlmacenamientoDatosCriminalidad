
CREATE TABLE IF NOT EXISTS H_DELITOS (
    ID_DANE_MUNICIPIO INTEGER,
    ID_TIPO_DELITO INTEGER,
    ARMAS_MEDIOS VARCHAR(100),
    FECHA_HECHO DATE NOT NULL,
    GENERO VARCHAR(30),
    AGRUPA_EDAD_PERSONA VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS DIM_TIPO_DELITOS (
    ID_TIPO_DELITO INTEGER,
    TIPO_DELITO VARCHAR(80) NOT NULL,
    CONSTRAINT ID_TIPO_DELITO PRIMARY KEY (ID_TIPO_DELITO)
);



CREATE TABLE IF NOT EXISTS DM_MUNICIPIOS (
    ID_DANE_MUNICIPIO INTEGER,
    ID_DANE_DEPARTAMENTO INTEGER,
    MUNICIPIO VARCHAR(80) NOT NULL,
    DEPARTAMENTO VARCHAR(80) NOT NULL,
    LATITUD_MUNICIPIO FLOAT NOT NULL,
    LONGITUD_MUNICIPIO FLOAT NOT NULL,
    CONSTRAINT ID_DANE_MUNICIPIO PRIMARY KEY (ID_DANE_MUNICIPIO)
);


ALTER TABLE H_DELITOS ADD CONSTRAINT H_DELITOS_fk0 FOREIGN KEY (ID_TIPO_DELITO) REFERENCES DIM_TIPO_DELITOS(ID_TIPO_DELITO);
ALTER TABLE H_DELITOS ADD CONSTRAINT H_DELITOS_fk1 FOREIGN KEY (ID_DANE_MUNICIPIO) REFERENCES DM_MUNICIPIOS(ID_DANE_MUNICIPIO);
