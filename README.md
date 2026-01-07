# PySpark Data Processing Lab

## 📌 Descripción
Este proyecto demuestra cómo usar **PySpark** para construir un flujo de procesamiento de datos distribuido.  
Se trabajan dos datasets en formato CSV, aplicando transformaciones, joins, agregaciones y exportando resultados a distintos formatos (Hive, Parquet y CSV).  

El objetivo es mostrar habilidades prácticas en **ETL con PySpark**, incluyendo:
- Lectura de datos con inferencia de esquema.  
- Limpieza y transformación de columnas.  
- Joins entre DataFrames.  
- Agregaciones y cálculos por grupo.  
- Escritura de resultados en diferentes destinos.  

---

## ⚙️ Tecnologías utilizadas
- **Python 3.x**  
- **PySpark** (Spark SQL, DataFrames API)  
- **Hive** (para almacenamiento tabular)  
- **HDFS** (para exportar datos en Parquet y CSV)  

---

## 📂 Flujo de trabajo

├── Dataset1.csv
├── Dataset2.csv
│
├── Spark DataFrames
│   ├── Lectura de CSV
│   └── Inferencia de esquema
│
├── Transformaciones
│   ├── Renombrar columnas
│   ├── Agregar columnas
│   └── Filtrar registros
│
├── Join
│   └── Join por customer_id
│
├── Agregaciones
│   ├── sum
│   └── avg
│
└── Resultados
    ├── Hive table
    │   └── customer_totals
    ├── Parquet
    │   └── filtered_data.parquet
    └── CSV
        └── total_value_per_year.csv

---

## 🚀 Ejecución

### 1. Instalar dependencias
```bash
pip install pyspark findspark wget
