from Extract.extract import extract_data
from Transform.transform import transform_data
from Load.load import load_data

def main_workflow():
  # Extract data
  extracted_data = extract_data()
  
  # Transform data
  transformed_data = transform_data(extracted_data)
  
  # Load data
  load_data(transformed_data)

if __name__ == "__main__":
  main_workflow()
