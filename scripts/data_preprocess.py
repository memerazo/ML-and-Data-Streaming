import pandas as pd
import numpy as np
import os

column_rename_map = {
    'Country': 'Country',
    'Country or region': 'Country',
    'Happiness Rank': 'Rank',
    'Happiness.Rank': 'Rank',
    'Overall rank': 'Rank',

    'Happiness Score': 'Score',
    'Happiness.Score': 'Score',
    'Score': 'Score',

    'Economy (GDP per Capita)': 'GDP per capita',
    'Economy..GDP.per.Capita.': 'GDP per capita',
    'GDP per capita': 'GDP per capita',

    'Family': 'Social support',
    'Social support': 'Social support',

    'Health (Life Expectancy)': 'Healthy life expectancy',
    'Health..Life.Expectancy.': 'Healthy life expectancy',
    'Healthy life expectancy': 'Healthy life expectancy',

    'Freedom': 'Freedom to make life choices',
    'Freedom to make life choices': 'Freedom to make life choices',

    'Trust (Government Corruption)': 'Perceptions of corruption',
    'Trust..Government.Corruption.': 'Perceptions of corruption',
    'Perceptions of corruption': 'Perceptions of corruption',

    'Generosity': 'Generosity',
    'Dystopia Residual': 'Dystopia Residual',
    'Dystopia.Residual': 'Dystopia Residual'
}

def data(df, year):
     
     """
    Prepares a happiness dataset by standardizing column names, 
    adding a year column, and selecting required columns.
    
    Renames columns according to a predefined mapping, adds the specified year,
    and returns only the required columns for analysis. Includes 'Dystopia Residual'
    if present in the original DataFrame.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing happiness data
        year (int): Year to be assigned to all records in the dataset

    Returns:
        pd.DataFrame: Processed DataFrame with standardized column names,
                      year column, and selected columns
    """
     
     df = df.rename(columns=column_rename_map)
     df['Year'] = year
     
     required_columns = [
        'Score', 'GDP per capita', 'Social support',
        'Healthy life expectancy', 'Freedom to make life choices',
        'Generosity', 'Perceptions of corruption'
    ]
     if 'Dystopia Residual' in df.columns:
        required_columns.append('Dystopia Residual')
        

     return df[required_columns]


def load_and_prepare_data(base_path, years):
    """
    Loads and prepares happiness data from multiple CSV files (one per year),
    combining them into a single DataFrame.
    
    Iterates through specified years, loading corresponding CSV files from the base path,
    prepares each dataset using the prepare_data() function, and combines all successful
    loads into a single DataFrame. Handles missing files and processing errors gracefully.

    Parameters:
        base_path (str): Directory path where the annual CSV files are stored
        years (list): List of years to attempt loading (files should be named '{year}.csv')

    Returns:
        pd.DataFrame: Combined DataFrame containing all successfully loaded and processed data,
                      or an empty DataFrame if no files could be loaded

    Note:
        Prints status messages for each file processed, including any errors encountered
        or files not found. Returns empty DataFrame if no valid files were processed.
    """
    dataframes = []
    for year in years:
        path = f"{base_path}/{year}.csv"
        if not os.path.isfile(path):
            print(f"File not found for year {year}: {path}")
            continue
        try:
            df = pd.read_csv(path)
            df_prepared = data(df, year)
            dataframes.append(df_prepared)
            print(f"Year {year}: loaded and processed successfully.")
        except Exception as e:
            print(f"Error processing file {year}.csv: {e}")
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else:
        print("No data was loaded.")
        return pd.DataFrame()
    

def process_and_save_data(dfs, output_path, null_threshold=0.1):
    """
    Concatenates a list of DataFrames, creates a unique ID per year,
    removes the 'Dystopia Residual' column if it exists,
    creates a logarithmic 'Score' column,
    and saves the result to a CSV file.

    Parameters:
        dfs (list of pd.DataFrame): List of preprocessed DataFrames by year.
        output_path (str): Path to the output .csv file.
    """
    
    df = pd.concat(dfs, ignore_index=True)

    df['ID'] = (
        df.groupby('Year').cumcount() + 1
    ).astype(str).str.zfill(2)
    df['ID'] = df['Year'].astype(str) + df['ID']

    df = df.drop(columns=['Dystopia Residual'], errors='ignore')
    
    null_ratio = df.isnull().mean()
    cols_to_drop = null_ratio[null_ratio > null_threshold].index.tolist()
    if cols_to_drop:
        print(f"Deleted Columns. Have more than {null_threshold*100}% de nulos: {cols_to_drop}")
        df = df.drop(columns=cols_to_drop)

    df['Score_log'] = np.log1p(df['Score'])

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)

    print(f"Processed data saved to  {output_path}")


def process_happiness_data(base_path, years, output_path, null_threshold=0.1):

    """
    Executes the complete happiness data processing pipeline:
    1. Loads and prepares data from multiple year files
    2. Processes and cleans the combined dataset
    3. Saves the final processed data to a CSV file

    Parameters:
        base_path (str): Directory path where the annual CSV files are stored
        years (list): List of years to process (files should be named '{year}.csv')
        output_path (str): Path where the final processed data will be saved
        null_threshold (float, optional): Threshold for null values (0-1). Columns with 
                                        higher null ratios will be dropped. Default 0.1

    Returns:
        pd.DataFrame: The final processed DataFrame, or None if processing failed

    Example:
        >>> df = process_happiness_data(
                base_path='data/raw',
                years=[2015, 2016, 2017],
                output_path='data/processed/happiness_processed.csv'
            )
    """
    try:
        # Step 1: Load and prepare data from all years
        print("Loading and preparing data from individual years...")
        combined_df = load_and_prepare_data(base_path, years)
        
        if combined_df.empty:
            print("Warning: No data was loaded - check your input paths and files")
            return None
        
        # Step 2: Process the combined dataset
        print("\nProcessing combined dataset...")
        # Wrap in list to match process_and_save_data's expected input format
        process_and_save_data([combined_df], output_path, null_threshold)
        
        # Step 3: Return the processed DataFrame
        print("\nData processing completed successfully")
        
        final_columns = [
            'Score',
            'GDP per capita',
            'Social support',
            'Healthy life expectancy',
            'Freedom to make life choices',
            'Generosity',
            'Perceptions of corruption'
        ]
        combined_df = combined_df[final_columns]
        
        return combined_df
    
    except Exception as e:
        print(f"\nError in processing pipeline: {str(e)}")
        return None