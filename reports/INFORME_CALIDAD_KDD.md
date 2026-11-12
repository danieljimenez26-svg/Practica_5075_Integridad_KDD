# Informe de Calidad de Datos y Preparación KDD (ISO/IEC 25012)

**Fecha de generación:** 2026-11-12 20:09:19  
**Módulo:** Big Data Aplicado (Curso de Especialización en IA y Big Data - RD 279/2021)  
**Resultados de Aprendizaje:** RA3 (Integridad y saneamiento) y RA5 (Explotación en Business Intelligence)

---

## 1. Resumen Ejecutivo de la Pasarela de Calidad

El pipeline automatizado ha procesado el lote de entrada aplicando controles criptográficos SHA-256 previos a la ingesta y reglas de validación estadística y de dominio antes de su paso a la fase de Minería de Datos del proceso KDD.

### 1.1. Integridad Criptográfica (Fase 1)
* **Ficheros analizados en el lote:** 3
* **Ficheros íntegros aprobados a *Staging*:** 2
* **Ficheros manipulados aislados en *Quarantine*:** 1
* **Tasa de Integridad Criptográfica:** **66.67%**

### 1.2. Calidad y Completitud de Registros (Fase 2 - ISO/IEC 25012)
* **Registros totales cargados desde ficheros verificados:** 100
* **Registros segregados a cuarentena (`bad_records.csv`):** 13
* **Registros aptos exportados (`dataset_kdd_listo.csv`):** 87
* **Tasa de Completitud Inicial:** **97.0%**
* **Tasa de Validez Final del Lote:** **87.0%**

---

## 2. Perfilado Estadístico de Variables Clave (`importe_eur`)

Tras la eliminación de valores atípicos extremos (*outliers*) y registros fuera de dominio, la variable principal de negocio presenta la siguiente distribución:

| Métrica Estadística | Valor (EUR) |
| :--- | :---: |
| **Media aritmética** | 70.99 € |
| **Mediana (P50)** | 73.44 € |
| **Desviación estándar** | 26.34 € |
| **Percentil 25 (Q1)** | 53.8 € |
| **Percentil 75 (Q3)** | 85.34 € |

---

## 3. Justificación Técnica de Decisiones y Valor para BI

1. **Bloqueo preventivo por SHA-256:** El rechazo automático de `transacciones_tienda_sur.csv` evita la inyección de datos alterados o truncados durante la transferencia de red, garantizando la trazabilidad exigida por el criterio CE 3.b.
2. **Aislamiento no destructivo (*Quarantine*):** Ningún dato anómalo se elimina silenciosamente; tanto los ficheros corruptos como los registros individuales defectuosos quedan registrados en `quarantine_manifest.txt` para su auditoría en origen.
3. **Estandarización temporal (ISO 8601) y filtrado IQR:** La normalización de marcas temporales y la exclusión de *outliers* provocados por fallos de sensores evita sesgos en las agregaciones OLAP, cuadros de mando de BI y futuros modelos predictivos (CE 5.b).
