CREATE TABLE empleados (
  id_empleado VARCHAR PRIMARY KEY,
  nombre TEXT,
  apellido TEXT,
  area TEXT,
  cargo TEXT,
  fecha_ingreso DATE,
  tipo_contrato TEXT
);

CREATE TABLE movimientos_personal (
  id_movimiento VARCHAR PRIMARY KEY,
  id_empleado VARCHAR REFERENCES empleados(id_empleado),
  tipo_movimiento TEXT,
  fecha DATE,
  motivo TEXT
);

CREATE TABLE formaciones (
  id_formacion VARCHAR PRIMARY KEY,
  id_empleado VARCHAR REFERENCES empleados(id_empleado),
  curso TEXT,
  horas INTEGER,
  fecha DATE
);

CREATE TABLE evaluaciones_desempeno (
  id_evaluacion VARCHAR PRIMARY KEY,
  id_empleado VARCHAR REFERENCES empleados(id_empleado),
  puntaje NUMERIC(3,2),
  fecha DATE,
  evaluador TEXT
);
