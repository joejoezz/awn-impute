# AgWeatherNet operational imputing module

Imputes AWN observational data in real time by training a random forest estimator on historical observations at each station and predicting as real-time data comes into the AgWeatherNet API.
Computes QA/QC checks by comparing running means of the imputed variables to the observed. 
Creates a new continuous data product by filling in missing and flagged data with imputed variables. 

## Required Python packages

### Core functions
- pandas  
- scikit-learn
- AWNPy: My module that interacts with the AWN API https://github.com/joejoezz/AWNPy/

### Modules
- clean: brings database up to date with the AgWeatherNet API, which is necessary because stations are deprecated from time to time.
- download_obs: download obs beginning at the `start_time` specified in config file up to the most recent 15-min observation.
- train: train random forest model. Only needs to be done once. Estimators are only trained on sites with at least one year of data.
- predict: create the imputed database by making predictions with the estimators. 
- qaqc: compare imputed values to observations and generate QA flags when the rolling mean of the differences exceeds the criteria set in the config file.
- plot: make plots comparing the most recent predictions to observations at each site

## Running the program
- Modify the config file
- Run using the following syntax: `python run config.conf --clean --download --train --predict --qaqc --plot`
- General practice is to only train the model once (because it takes several days) and then simply predict off new data
