# **Workshop 3: Machine Learning and Data Streaming**  

## **Description**  
This project implements a pipeline to predict countries' happiness scores using regression models. It includes EDA, ETL, model training, Kafka streaming, database storage, and evaluation.

## **Project Structure**  
- `data/`: Raw and processed CSV files  
- `notebooks/`: Jupyter Notebooks for EDA, ETL, training, and evaluation  
- `scripts/`: Kafka and database scripts  
- `models/`: Trained model (.pkl)  
- `database/`: Database with predictions  
- `config/`: Configuration files  
- `results/`: Metrics and graphs  
- `deliverable/`: Final deliverables  

## **Installation**  
1. Clone the repository  
2. Install dependencies: `pip install -r requirements.txt`  
3. Configure Kafka and database (see `config/`)  
4. Run notebooks in order:  
   - `eda.ipynb` → `etl.ipynb` → `model_training.ipynb` → `evaluation.ipynb`  
5. Execute Kafka scripts:  
   - `kafka_producer.py` (data simulation)  
   - `kafka_consumer.py` (prediction storage)  

## **Deliverables**  
- Final notebook: `deliverable/workshop3_final_notebook.html`  
- Trained model: `deliverable/happiness_model.pkl`  
- Database: `deliverable/happiness_predictions.db`  
- Metrics: `deliverable/metrics.txt`  

