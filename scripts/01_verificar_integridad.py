import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"

STAGING_VERIFIED_DIR = BASE_DIR / "workspace" / "staging" / "verified"
QUARANTINE_TAMPERED_DIR = BASE_DIR / "workspace" / "quarantine" / "tampered"

LOGS_DIR = BASE_DIR / "logs"
OUTPUT_SAMPLES_DIR = BASE_DIR / "output_samples"
LOG_FILE = LOGS_DIR / "ejecucion_pipeline.log"
QUARANTINE_MANIFEST = OUTPUT_SAMPLES_DIR / "quarantine_manifest.txt"


def configurar_logging() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [FASE-1-CRYPTO] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def calcular_sha256(ruta_fichero: Path) -> str:
    hasher = hashlib.sha256()
    with open(ruta_fichero, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def ejecutar_verificacion() -> None:
    configurar_logging()
    logging.info("Iniciando Pasarela de Integridad Criptográfica (SHA-256)...")

    STAGING_VERIFIED_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_TAMPERED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    if not MANIFEST_PATH.exists():
        logging.error(f"No se encuentra el manifiesto oficial en: {MANIFEST_PATH}")
        return

    archivos_validados = 0
    archivos_rechazados = 0
    registros_cuarentena = []

    with open(MANIFEST_PATH, "r", encoding="utf-8") as manifest:
        for num_linea, linea in enumerate(manifest, start=1):
            linea = linea.strip()
            if not linea:
                continue

            partes = linea.split()
            if len(partes) != 2:
                logging.warning(f"Línea {num_linea} mal formada en manifiesto: {linea}")
                continue

            hash_esperado, nombre_archivo = partes[0].lower(), partes[1]
            ruta_origen = RAW_DIR / nombre_archivo

            if not ruta_origen.exists():
                logging.error(f"Archivo ausente en lote de entrada: {nombre_archivo}")
                archivos_rechazados += 1
                registros_cuarentena.append(
                    f"{datetime.now().isoformat()} | {nombre_archivo} | ARCHIVO_AUSENTE | Esperado: {hash_esperado}"
                )
                continue

            hash_calculado = calcular_sha256(ruta_origen).lower()

            if hash_calculado == hash_esperado:
                destino = STAGING_VERIFIED_DIR / nombre_archivo
                shutil.copy2(ruta_origen, destino)
                archivos_validados += 1
                logging.info(
                    f"OK -> {nombre_archivo} | Hash verificado ({hash_calculado[:12]}...). Movido a staging/verified/"
                )
            else:
                destino = QUARANTINE_TAMPERED_DIR / nombre_archivo
                shutil.copy2(ruta_origen, destino)
                archivos_rechazados += 1
                logging.critical(
                    f"ALERTA DE INTEGRIDAD -> {nombre_archivo} RECHAZADO. "
                    f"Esperado: {hash_esperado[:12]}... | Calculado: {hash_calculado[:12]}... "
                    f"Aislado en quarantine/tampered/"
                )
                registros_cuarentena.append(
                    f"{datetime.now().isoformat()} | {nombre_archivo} | HASH_MISMATCH | "
                    f"Esperado: {hash_esperado} | Calculado: {hash_calculado}"
                )

    with open(QUARANTINE_MANIFEST, "w", encoding="utf-8") as qf:
        qf.write("# MANIFIESTO DE CUARENTENA - CONTROL DE INTEGRIDAD Y CALIDAD\n")
        qf.write(f"# Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        qf.write("=" * 80 + "\n")
        if registros_cuarentena:
            for reg in registros_cuarentena:
                qf.write(reg + "\n")
        else:
            qf.write("Sin incidencias criptográficas en Fase 1.\n")

    logging.info(
        f"Resumen Fase 1: {archivos_validados} aprobados, {archivos_rechazados} en cuarentena."
    )


if __name__ == "__main__":
    ejecutar_verificacion()
