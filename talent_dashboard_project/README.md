# Talent & Culture Dashboard Project

Este proyecto automatiza la carga mensual de archivos Excel a PostgreSQL y provee un DAG de Airflow para gestionar el proceso ETL. Los datos se utilizan para un dashboard en Tableau.

## Estructura

```
talent_dashboard_project/
├── dags/
│   └── load_talent_data.py
├── etl/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── data/
│   ├── raw_data/
│   └── processed/
├── logs/
├── sql/
│   └── schema.sql
├── generate_dummy_data.py
├── .env
├── requirements.txt
└── README.md
```

## Instalación

1. Crear un entorno virtual e instalar dependencias:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Configurar la conexión a PostgreSQL en el archivo `.env`:

```
DATABASE_URL=postgresql://user:password@localhost:5432/talent_db
```

3. Crear las tablas ejecutando `sql/schema.sql` en la base de datos.

## Generar datos de prueba

Ejecutar:

```bash
python generate_dummy_data.py
```

Los archivos se guardarán en `data/raw_data/`.

## Ejecutar el DAG

Copiar el archivo `dags/load_talent_data.py` al directorio de DAGs de Airflow y arrancar Airflow. El DAG buscará nuevos archivos en `data/raw_data/`, los cargará en PostgreSQL y moverá los procesados a `data/processed/`.

## Pruebas

Las pruebas unitarias se ejecutan con:

```bash
pytest
```

## Visualización en Tableau

1. Conecta Tableau a la base de datos PostgreSQL usando la URL definida en
   `.env`.
2. También puedes generar un archivo `.hyper` para importarlo en Tableau
   ejecutando:

   ```bash
   python etl/export_to_hyper.py
   ```

   El archivo se guardará en `data/tableau/talent_data.hyper`.
3. Usa ese origen de datos para crear gráficos y KPI en Tableau.
