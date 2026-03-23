import pandas as pd

def validar_contratos(ruta):
    errores = []

    df = pd.read_excel(ruta)

    df.columns = df.columns.str.strip()

    columnas_requeridas = [
        "Sucursal",
        "Cajero",
        "ID_Transaccion",
        "Transaccion",
        "Tiempo_Servicio_seg",
        "Satisfaccion",
        "Monto"
    ]

    for col in columnas_requeridas:
        if col not in df.columns:
            errores.append({
                "tipo": f"Falta columna: {col}",
                "filas": []
            })

    return errores, df