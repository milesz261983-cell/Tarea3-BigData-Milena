# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka

**Estudiante:** Milena Salazar Sánchez
**Curso:** Big Data - Grupo 150

## Descripción del Proyecto
Este repositorio contiene el código desarrollado para la Tarea 3, dividido en dos partes:
1. **Procesamiento en Batch (Spark):** Análisis del dataset público "Online Retail" para identificar los productos más vendidos y los ingresos por país.
2. **Procesamiento en Tiempo Real (Kafka + Spark Streaming):** Simulación de datos de sensores IoT y cálculo de promedios en ventanas de tiempo.

## Archivos del Repositorio
- `batch_retail.py`: Código para el análisis de ventas en batch.
- `kafka_producer.py`: Script que simula y envía los datos de los sensores a Kafka.
- `spark_streaming_consumer.py`: Script de Spark que lee los datos de Kafka y calcula estadísticas en tiempo real.

## Tecnologías Utilizadas
- Python 3
- Apache Spark 3.5.3
- Apache Kafka 3.9.2
- WSL2 (Ubuntu 22.04)
