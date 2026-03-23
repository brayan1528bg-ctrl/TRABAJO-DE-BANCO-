import os
import shutil
import pandas as pd
from validate import validar_contratos

print("🔥 SCRIPT INICIADO")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUTA_RAW = os.path.join(BASE_DIR, "data", "raw", "banco.xlsx")
RUTA_STAGING = os.path.join(BASE_DIR, "data", "staging", "banco.xlsx")
RUTA_QUARANTINE = os.path.join(BASE_DIR, "data", "quarantine", "banco.xlsx")
RUTA_REPORTE = os.path.join(BASE_DIR, "reports", "errores_banco.xlsx")


def asegurar_directorios():
    rutas = [
        os.path.dirname(RUTA_STAGING),
        os.path.dirname(RUTA_QUARANTINE),
        os.path.dirname(RUTA_REPORTE),
    ]
    for ruta in rutas:
        os.makedirs(ruta, exist_ok=True)


def ejecutar_pipeline():
    print("🚀 Ejecutando pipeline...")

    asegurar_directorios()

    # VALIDACIÓN
    try:
        errores, df = validar_contratos(RUTA_RAW)
        print("📊 Archivo leído correctamente")
    except Exception as e:
        print(f"❌ Error: {e}")
        return

    # SI HAY ERRORES
    if errores:
        print("❌ Archivo con errores")

        shutil.copy(RUTA_RAW, RUTA_QUARANTINE)

        filas_error = []
        for error in errores:
            for fila in error.get("filas", []):
                filas_error.append({
                    "fila": fila,
                    "error": error.get("tipo")
                })

        df_errores = pd.DataFrame(filas_error)

        if not df_errores.empty:
            df_errores.to_excel(RUTA_REPORTE, index=False)

        print("📁 Enviado a quarantine")
        print("📄 Reporte generado")
        return

    # PROCESAMIENTO
    print("✅ Datos válidos")

    try:
        df.columns = df.columns.str.strip()

        df = df.drop_duplicates(subset=["ID_Transaccion"])
        df["Monto"] = pd.to_numeric(df["Monto"], errors="coerce")
        df = df.dropna(subset=["Monto"])

        df.to_excel(RUTA_STAGING, index=False)

        print("🎯 PROCESO FINALIZADO")
        print(f"📁 Archivo final en: {RUTA_STAGING}")

    except Exception as e:
        print(f"❌ Error procesando: {e}")


if __name__ == "__main__":
    ejecutar_pipeline()