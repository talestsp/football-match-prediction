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

# Data Types
* Schema
  * All data types can be found here: data/schema.json
* Parsing
  * Datetime columns has been loaded as DateType
  * Boolean data types that we originally filled with 1 and 0 were parsed to True and False

# Train, Test and Validation split

