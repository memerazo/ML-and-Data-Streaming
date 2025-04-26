# Workshop 3: Machine Learning y Data Streaming

## Descripción
Este proyecto implementa un pipeline para predecir la puntuación de felicidad de países usando un modelo de regresión. Incluye EDA, ETL, entrenamiento de modelo, streaming con Kafka, almacenamiento en base de datos y evaluación.

## Estructura del Proyecto
- `data/`: Archivos CSV y datos procesados.
- `notebooks/`: Jupyter Notebooks para EDA, ETL, entrenamiento y evaluación.
- `scripts/`: Scripts para Kafka y base de datos.
- `models/`: Modelo entrenado (.pkl).
- `database/`: Base de datos con predicciones.
- `config/`: Archivos de configuración.
- `results/`: Métricas y gráficos.
- `deliverable/`: Archivos finales para la entrega.

## Instalación
1. Clona el repositorio.
2. Instala las dependencias: `pip install -r requirements.txt`.
3. Configura Kafka y la base de datos según `config/`.
4. Ejecuta los notebooks en orden: `eda.ipynb`, `etl.ipynb`, `model_training.ipynb`, `evaluation.ipynb`.
5. Ejecuta los scripts de Kafka: `kafka_producer.py` y `kafka_consumer.py`.

## Entregables
- Notebook final: `deliverable/workshop3_final_notebook.html`
- Modelo: `deliverable/happiness_model.pkl`
- Base de datos: `deliverable/happiness_predictions.db`
- Métricas: `deliverable/metrics.txt`