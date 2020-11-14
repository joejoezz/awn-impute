# AWN operational imputing module

Imputes AWN observational data in real time by training a random forest estimator on historical observations at each station and predicting as real-time data comes into the AgWeatherNet API.
Computes QA/QC checks by comparing running means of the imputed variables to the observed. 
Creates a new continuous data product by filling in missing and flagged data with imputed variables. 

## Required Python packages

### Core functions
- pandas  
- scikit-learn
- AWNPy (my module that interacts with the AWN API)

## Running the program
- Modify the config file
- Run using the following syntax: `python run config.conf --clean --download --train --predict --qaqc --plot`
- General practice is to only train the model once (because it takes several days) and then simply predict off new data
