import pandas as pd
from pathlib import Path

RAW_DATA_DIR = Path(__file__).parent / 'data' / 'raw_data'
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Dummy data for empleados
empleados = pd.DataFrame([
    {
        'id_empleado': f'E{str(i).zfill(3)}',
        'nombre': f'Nombre{i}',
        'apellido': f'Apellido{i}',
        'area': 'Area' + str(i % 3 + 1),
        'cargo': 'Cargo' + str(i % 5 + 1),
        'fecha_ingreso': '2024-01-{:02d}'.format((i % 12) + 1),
        'tipo_contrato': 'Indefinido' if i % 2 == 0 else 'Temporal'
    }
    for i in range(1, 11)
])

movimientos = pd.DataFrame([
    {
        'id_movimiento': f'M{str(i).zfill(3)}',
        'id_empleado': empleados.loc[i % len(empleados), 'id_empleado'],
        'tipo_movimiento': 'Ingreso' if i % 2 == 0 else 'Egreso',
        'fecha': '2025-05-{:02d}'.format((i % 30) + 1),
        'motivo': 'Motivo' + str(i)
    }
    for i in range(1, 6)
])

formaciones = pd.DataFrame([
    {
        'id_formacion': f'F{str(i).zfill(3)}',
        'id_empleado': empleados.loc[i % len(empleados), 'id_empleado'],
        'curso': 'Curso' + str(i),
        'horas': 10 + i,
        'fecha': '2025-05-{:02d}'.format((i % 30) + 1)
    }
    for i in range(1, 6)
])

evaluaciones = pd.DataFrame([
    {
        'id_evaluacion': f'EVAL{str(i).zfill(3)}',
        'id_empleado': empleados.loc[i % len(empleados), 'id_empleado'],
        'puntaje': round(3 + i * 0.1, 2),
        'fecha': '2025-05-{:02d}'.format((i % 30) + 1),
        'evaluador': f'Evaluador{i}'
    }
    for i in range(1, 6)
])

files = {
    'empleados_2025-05.xlsx': empleados,
    'movimientos_2025-05.xlsx': movimientos,
    'formaciones_2025-05.xlsx': formaciones,
    'evaluaciones_2025-05.xlsx': evaluaciones
}

for filename, df in files.items():
    path = RAW_DATA_DIR / filename
    df.to_excel(path, index=False)
    print(f'Generated {path}')
