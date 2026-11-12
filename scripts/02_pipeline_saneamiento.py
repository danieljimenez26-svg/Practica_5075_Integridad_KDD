import json
import logging
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
STAGING_VERIFIED_DIR = BASE_DIR / "workspace" / "staging" / "verified"
QUARANTINE_DIR = BASE_DIR / "workspace" / "quarantine"
PROCESSED_DIR = BASE_DIR / "workspace" / "processed"

LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"
OUTPUT_SAMPLES_DIR = BASE_DIR / "output_samples"

LOG_FILE = LOGS_DIR / "ejecucion_pipeline.log"
QUARANTINE_MANIFEST = OUTPUT_SAMPLES_DIR / "quarantine_manifest.txt"
METRICS_JSON_PATH = OUTPUT_SAMPLES_DIR / "metricas_perfilado.json"
REPORT_MD_PATH = REPORTS_DIR / "INFORME_CALIDAD_KDD.md"


def configurar_logging() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [FASE-2-3-KDD] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def cargar_datos_verificados() -> pd.DataFrame:
    archivos_csv = sorted(STAGING_VERIFIED_DIR.glob("*.csv"))
    if not archivos_csv:
        raise FileNotFoundError(
            f"No hay archivos CSV verificados en {STAGING_VERIFIED_DIR}. Ejecuta primero 01_verificar_integridad.py."
        )

    dataframes = []
    for archivo in archivos_csv:
        logging.info(f"Cargando fichero verificado: {archivo.name}")
        df_temp = pd.read_csv(archivo)
        df_temp["origen_fichero"] = archivo.name
        dataframes.append(df_temp)

    df_total = pd.concat(dataframes, ignore_index=True)
    logging.info(f"Total registros cargados en bruto desde staging: {len(df_total)}")
    return df_total


def ejecutar_pipeline_kdd() -> None:
    configurar_logging()
    logging.info("Iniciando Fase 2 (Saneamiento y Outliers) y Fase 3 (Perfilado KDD)...")

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    df = cargar_datos_verificados()
    total_inicial = len(df)
    registros_malos = []

    columnas_obligatorias = ["id_transaccion", "timestamp", "importe_eur", "unidades"]
    faltantes = [c for c in columnas_obligatorias if c not in df.columns]
    if faltantes:
        raise ValueError(f"Schema Drift detectado. Faltan columnas críticas: {faltantes}")

    filas_sin_nulos_inicial = int(df[columnas_obligatorias].dropna().shape[0])
    tasa_completitud_inicial = round((filas_sin_nulos_inicial / total_inicial) * 100, 2) if total_inicial else 0.0

    mask_nulos_criticos = df["id_transaccion"].isna() | df["importe_eur"].isna() | df["timestamp"].isna()
    if mask_nulos_criticos.any():
        df_nulos = df[mask_nulos_criticos].copy()
        df_nulos["motivo_rechazo"] = "NULO_EN_CAMPO_CRITICO"
        registros_malos.append(df_nulos)
        logging.warning(f"Segregados {len(df_nulos)} registros por valores nulos críticos.")
        df = df[~mask_nulos_criticos].copy()

    if df["unidades"].isna().any():
        mediana_unidades = float(df["unidades"].median())
        n_imputados = int(df["unidades"].isna().sum())
        df["unidades"] = df["unidades"].fillna(mediana_unidades)
        logging.info(f"Imputados {n_imputados} nulos en 'unidades' usando la mediana ({mediana_unidades}).")

    mask_duplicados = df.duplicated(subset=["id_transaccion"], keep="first")
    if mask_duplicados.any():
        df_dup = df[mask_duplicados].copy()
        df_dup["motivo_rechazo"] = "ID_TRANSACCION_DUPLICADO"
        registros_malos.append(df_dup)
        logging.warning(f"Segregados {len(df_dup)} registros duplicados.")
        df = df[~mask_duplicados].copy()

    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
    ahora = pd.Timestamp.now()
    inicio_anio = pd.Timestamp(year=ahora.year, month=1, day=1)

    mask_fecha_invalida = (
        df["timestamp_dt"].isna()
        | (df["timestamp_dt"] > ahora)
        | (df["timestamp_dt"] < inicio_anio)
    )
    if mask_fecha_invalida.any():
        df_fechas = df[mask_fecha_invalida].drop(columns=["timestamp_dt"]).copy()
        df_fechas["motivo_rechazo"] = "TIMESTAMP_CORRUPTO_O_FUERA_DE_RANGO"
        registros_malos.append(df_fechas)
        logging.warning(f"Segregados {len(df_fechas)} registros por anomalía temporal.")
        df = df[~mask_fecha_invalida].copy()

    df["timestamp"] = df["timestamp_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.drop(columns=["timestamp_dt"])

    df["importe_eur"] = pd.to_numeric(df["importe_eur"], errors="coerce")
    df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce")

    mask_dominio = (df["importe_eur"] <= 0) | (df["unidades"] < 1) | df["importe_eur"].isna() | df["unidades"].isna()
    if mask_dominio.any():
        df_dom = df[mask_dominio].copy()
        df_dom["motivo_rechazo"] = "VIOLACION_REGLA_DOMINIO"
        registros_malos.append(df_dom)
        logging.warning(f"Segregados {len(df_dom)} registros por valores imposibles (importe <= 0 o unidades < 1).")
        df = df[~mask_dominio].copy()

    q1 = df["importe_eur"].quantile(0.25)
    q3 = df["importe_eur"].quantile(0.75)
    iqr = q3 - q1
    limite_inferior = q1 - 1.5 * iqr
    limite_superior = q3 + 1.5 * iqr

    mask_outliers = (df["importe_eur"] < limite_inferior) | (df["importe_eur"] > limite_superior)
    if mask_outliers.any():
        df_out = df[mask_outliers].copy()
        df_out["motivo_rechazo"] = f"OUTLIER_ESTADISTICO_IQR (>{limite_superior:.2f} EUR)"
        registros_malos.append(df_out)
        logging.warning(
            f"Segregados {len(df_out)} outliers estadísticos en 'importe_eur' (umbral superior IQR: {limite_superior:.2f} EUR)."
        )
        df = df[~mask_outliers].copy()

    total_bad_records = 0
    if registros_malos:
        df_bad_total = pd.concat(registros_malos, ignore_index=True)
        total_bad_records = len(df_bad_total)
        bad_records_path = QUARANTINE_DIR / "bad_records.csv"
        df_bad_total.to_csv(bad_records_path, index=False, encoding="utf-8")
        logging.info(f"Registros anómalos exportados a: {bad_records_path}")

        with open(QUARANTINE_MANIFEST, "a", encoding="utf-8") as qf:
            qf.write("\n# REGISTROS SEGREGADOS EN FASE 2 (SANEAMIENTO DE DATOS)\n")
            for _, row in df_bad_total.iterrows():
                qf.write(
                    f"ID: {row.get('id_transaccion', 'N/A')} | Origen: {row.get('origen_fichero', 'N/A')} | "
                    f"Motivo: {row['motivo_rechazo']}\n"
                )

    output_dataset_path = PROCESSED_DIR / "dataset_kdd_listo.csv"
    df.to_csv(output_dataset_path, index=False, encoding="utf-8")
    logging.info(f"Dataset limpio exportado a: {output_dataset_path} ({len(df)} registros válidos).")

    total_final = len(df)
    tasa_validez = round((total_final / total_inicial) * 100, 2) if total_inicial else 0.0

    n_validados_f1 = len(list(STAGING_VERIFIED_DIR.glob("*.csv")))
    n_rechazados_f1 = len(list((QUARANTINE_DIR / "tampered").glob("*.csv")))
    total_ficheros_f1 = n_validados_f1 + n_rechazados_f1
    tasa_integridad_f1 = round((n_validados_f1 / total_ficheros_f1) * 100, 2) if total_ficheros_f1 else 0.0

    metricas = {
        "estandar_referencia": "ISO/IEC 25012",
        "fecha_perfilado": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "integridad_criptografica_fase1": {
            "ficheros_totales": total_ficheros_f1,
            "ficheros_aprobados": n_validados_f1,
            "ficheros_cuarentena": n_rechazados_f1,
            "tasa_integridad_pct": tasa_integridad_f1,
        },
        "calidad_registros_fase2": {
            "registros_brutos_staging": total_inicial,
            "registros_segregados_cuarentena": total_bad_records,
            "registros_validos_kdd": total_final,
            "tasa_completitud_inicial_pct": tasa_completitud_inicial,
            "tasa_validez_final_pct": tasa_validez,
        },
        "estadisticos_importe_eur_limpio": {
            "media": round(float(df["importe_eur"].mean()), 2) if total_final else 0.0,
            "mediana": round(float(df["importe_eur"].median()), 2) if total_final else 0.0,
            "desviacion_estandar": round(float(df["importe_eur"].std()), 2) if total_final > 1 else 0.0,
            "percentil_25": round(float(df["importe_eur"].quantile(0.25)), 2) if total_final else 0.0,
            "percentil_75": round(float(df["importe_eur"].quantile(0.75)), 2) if total_final else 0.0,
        },
    }

    with open(METRICS_JSON_PATH, "w", encoding="utf-8") as jf:
        json.dump(metricas, jf, indent=4, ensure_ascii=False)
    logging.info(f"Métricas de perfilado guardadas en: {METRICS_JSON_PATH}")

    generar_informe_markdown(metricas)
    logging.info(f"Informe técnico generado en: {REPORT_MD_PATH}")


def generar_informe_markdown(m: dict) -> None:
    f1 = m["integridad_criptografica_fase1"]
    f2 = m["calidad_registros_fase2"]
    est = m["estadisticos_importe_eur_limpio"]

    contenido = f"""# Informe de Calidad de Datos y Preparación KDD (ISO/IEC 25012)

**Fecha de generación:** {m['fecha_perfilado']}  
**Módulo:** Big Data Aplicado (Curso de Especialización en IA y Big Data - RD 279/2021)  
**Resultados de Aprendizaje:** RA3 (Integridad y saneamiento) y RA5 (Explotación en Business Intelligence)

---

## 1. Resumen Ejecutivo de la Pasarela de Calidad

El pipeline automatizado ha procesado el lote de entrada aplicando controles criptográficos SHA-256 previos a la ingesta y reglas de validación estadística y de dominio antes de su paso a la fase de Minería de Datos del proceso KDD.

### 1.1. Integridad Criptográfica (Fase 1)
* **Ficheros analizados en el lote:** {f1['ficheros_totales']}
* **Ficheros íntegros aprobados a *Staging*:** {f1['ficheros_aprobados']}
* **Ficheros manipulados aislados en *Quarantine*:** {f1['ficheros_cuarentena']}
* **Tasa de Integridad Criptográfica:** **{f1['tasa_integridad_pct']}%**

### 1.2. Calidad y Completitud de Registros (Fase 2 - ISO/IEC 25012)
* **Registros totales cargados desde ficheros verificados:** {f2['registros_brutos_staging']}
* **Registros segregados a cuarentena (`bad_records.csv`):** {f2['registros_segregados_cuarentena']}
* **Registros aptos exportados (`dataset_kdd_listo.csv`):** {f2['registros_validos_kdd']}
* **Tasa de Completitud Inicial:** **{f2['tasa_completitud_inicial_pct']}%**
* **Tasa de Validez Final del Lote:** **{f2['tasa_validez_final_pct']}%**

---

## 2. Perfilado Estadístico de Variables Clave (`importe_eur`)

Tras la eliminación de valores atípicos extremos (*outliers*) y registros fuera de dominio, la variable principal de negocio presenta la siguiente distribución:

| Métrica Estadística | Valor (EUR) |
| :--- | :---: |
| **Media aritmética** | {est['media']} € |
| **Mediana (P50)** | {est['mediana']} € |
| **Desviación estándar** | {est['desviacion_estandar']} € |
| **Percentil 25 (Q1)** | {est['percentil_25']} € |
| **Percentil 75 (Q3)** | {est['percentil_75']} € |

---

## 3. Justificación Técnica de Decisiones y Valor para BI

1. **Bloqueo preventivo por SHA-256:** El rechazo automático de `transacciones_tienda_sur.csv` evita la inyección de datos alterados o truncados durante la transferencia de red, garantizando la trazabilidad exigida por el criterio CE 3.b.
2. **Aislamiento no destructivo (*Quarantine*):** Ningún dato anómalo se elimina silenciosamente; tanto los ficheros corruptos como los registros individuales defectuosos quedan registrados en `quarantine_manifest.txt` para su auditoría en origen.
3. **Estandarización temporal (ISO 8601) y filtrado IQR:** La normalización de marcas temporales y la exclusión de *outliers* provocados por fallos de sensores evita sesgos en las agregaciones OLAP, cuadros de mando de BI y futuros modelos predictivos (CE 5.b).
"""
    with open(REPORT_MD_PATH, "w", encoding="utf-8") as rf:
        rf.write(contenido)


if __name__ == "__main__":
    ejecutar_pipeline_kdd()
