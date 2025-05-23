import json
import pandas as pd
import numpy as np
import os
from sqlalchemy import text
import joblib
from kafka import KafkaConsumer
from sklearn.metrics import mean_squared_error, r2_score
from sqlalchemy import create_engine
import time

def create_db_engine():
    try:
        # Load credentials from JSON file
        credentials_path = '/home/user/workshop3_ml_datastreaming/config/credentials.json'
        with open(credentials_path, 'r') as f:
            creds = json.load(f)['database']

        # Construct connection string
        user = creds['user']
        password = creds['password']
        host = creds['host']
        port = creds['port']
        dbname = creds['dbname']

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(db_url)
        print("✅ PostgreSQL engine created.")
        return engine

    except Exception as e:
        print(f"❌ Error creating DB engine: {e}")
        return None

def setup_database(engine):
    with engine.connect() as conn:
        conn.execute(text("""
                          CREATE TABLE happiness_predictions (
                            id SERIAL PRIMARY KEY,
                            gdp_per_capita DOUBLE PRECISION,
                            healthy_life_expectancy DOUBLE PRECISION,
                            freedom_to_make_life_choices DOUBLE PRECISION,
                            generosity DOUBLE PRECISION,
                            perceptions_of_corruption DOUBLE PRECISION,
                            continent_africa INTEGER,
                            continent_america INTEGER,
                            continent_europe INTEGER,
                            predicted_happiness_score DOUBLE PRECISION
            );
        """))
    print("Database setup complete.")

def inv_boxcox(y_transformed, lambda_):
    if lambda_ == 0:
        return np.exp(y_transformed) - 1
    else:
        return np.power(lambda_ * y_transformed + 1, 1 / lambda_) - 1

def save_predictions(predictions, output_path, engine=None):
    df = pd.DataFrame(predictions)
    
    # Renombrar columnas como antes
    df.rename(columns={
        'GDP per capita': 'gdp_per_capita',
        'Healthy life expectancy': 'healthy_life_expectancy',
        'Freedom to make life choices': 'freedom_to_make_life_choices',
        'Generosity': 'generosity',
        'Perceptions of corruption': 'perceptions_of_corruption',
        'Continent_Africa': 'continent_africa',
        'Continent_America': 'continent_america',
        'Continent_Europe': 'continent_europe',
        'Predicted_Happiness_Score': 'predicted_happiness_score'
    }, inplace=True)
    
    # Agrega score real si está en los datos
    if 'Score' in df.columns:
        df.rename(columns={'Score': 'score'}, inplace=True)

    print(f"Trying to save {len(df)} rows to the database.")
    print("Data preview:")
    print(df.head())

    # Guardar CSV como backup
    df.to_csv(output_path, mode='a', header=not os.path.exists(output_path), index=False)

    if engine:
        try:
            df.to_sql('happiness_predictions', con=engine, if_exists='append', index=False)
            print("📥 Data inserted into PostgreSQL successfully.")
        except Exception as e:
            print(f"⚠️ Error inserting into DB: {e}")
    else:
        print("⚠️ No DB engine provided. Skipping DB insert.")



def run_consumer(
    topic='happiness_data',
    bootstrap_servers=['localhost:9092'],
    model_path='/home/user/workshop3_ml_datastreaming/models/catboost_boxcox_bundle.pkl',
    original_data_path='/home/user/workshop3_ml_datastreaming/data/processed/cleaned_data.csv',
    output_path='/home/user/workshop3_ml_datastreaming/results/streaming_predictions.csv'
):
    print("Starting consumer...")


    # Cargar modelo
    model_data = joblib.load(model_path)
    model = model_data['model'] if isinstance(model_data, dict) else model_data
    lambda_ = model_data['lambda_']
    print("✅ Model loaded.")

    # Cargar dataframe original para comparar score real
    df_original = pd.read_csv(original_data_path)

    # Conexión base de datos
    engine = create_db_engine()
    if engine:
        setup_database(engine)
    else:
        print("⚠️ No se pudo conectar a la base de datos, se omitirán inserciones en DB.")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=10000  # termina si no recibe mensajes en 10s
    )

    features = [
        'GDP per capita', 'Healthy life expectancy', 'Freedom to make life choices',
        'Generosity', 'Perceptions of corruption',
        'Continent_Africa', 'Continent_America', 'Continent_Europe'
    ]

    predictions = []
    actuals = []

    try:
        for message in consumer:
            try:
                data = message.value
                print(f"Received message: {data}")

                # Verificar si tiene las features
                if not all(f in data for f in features):
                    print("⚠️ Mensaje con features incompletas, se ignora:", data)
                    continue

                X = pd.DataFrame([data])[features]
                pred_transformed = model.predict(X)[0]  # Predicción en escala transformada (Box-Cox)

                # --- Aquí va la transformación inversa Box-Cox ---
                if lambda_ is not None:
                    if lambda_ == 0:
                        pred_original = np.exp(pred_transformed) - 1
                    else:
                        pred_original = np.power((pred_transformed * lambda_) + 1, 1 / lambda_) - 1
                else:
                    pred_original = pred_transformed  # fallback si no tienes lambda

                data['Predicted_Happiness_Score'] = pred_original
                predictions.append(data)

                mask = True
                for f in features:
                    mask &= (df_original[f] == data[f])
                match = df_original[mask]

                if not match.empty:
                    actual_score = match.iloc[0]['Score']
                    actuals.append({'Score': actual_score, 'Predicted_Happiness_Score': pred_original})

                # Guardar cada 10 predicciones
                if len(predictions) >= 10:
                    save_predictions(predictions, output_path, engine)
                    predictions.clear()

            except Exception as e_inner:
                print(f"Error procesando mensaje: {e_inner}")

        # Guardar lo que queda
        if predictions:
            save_predictions(predictions, output_path, engine)

        # Calcular métricas
        if actuals:
            df_actuals = pd.DataFrame(actuals)
            rmse = np.sqrt(mean_squared_error(df_actuals['Score'], df_actuals['Predicted_Happiness_Score']))
            r2 = r2_score(df_actuals['Score'], df_actuals['Predicted_Happiness_Score'])
            print("\n📊 === Métricas de streaming ===")
            print(f"RMSE: {rmse:.4f}")
            print(f"R²: {r2:.4f}")
            df_actuals.to_csv('/home/user/workshop3_ml_datastreaming/results/streaming_metrics_predictions.csv', index=False)
            print("Métricas guardadas en ../results/streaming_metrics.csv")

    except Exception as e:
        print(f"Error fatal en consumer: {e}")

    finally:
        consumer.close()
        print("Consumer cerrado.")

if __name__ == "__main__":
    run_consumer()

