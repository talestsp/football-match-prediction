# football-match-prediction
Repository to work on [Kaggle's Football Match Probability Prediction challenge](https://www.kaggle.com/competitions/football-match-probability-prediction/).

# Project Structure
* data/
  * raw/     <- The original, immutable data dump
  * interim/  <- Intermediate data that has been transformed

* src/
  * dao/
  * utils/
    * dflib.py
    * plot.py
    * pretties.py
    * stats.py

* notebooks/
  * [DataUnderstanding.ipynb](notebooks/DataUnderstanding.ipynb)
  * [SplitData.ipynb](notebooks/SplitData.ipynb)

# Data Types
* Schema
  * All data types can be found here: data/schema.json
* Parsing
  * Datetime columns has been loaded as DateType
  * Boolean data types that we originally filled with 1 and 0 were parsed to True and False

# Train, Test and Validation split
The adopted strategy for splitting train and validation datasets can be found at [SplitData.ipynb](notebooks/SplitData.ipynb) notebook. As the test dataset represents a slice made in time, the validation dataset was splitted in the same way.
The 20% most recent data from training was partitioned to validation dataset.

