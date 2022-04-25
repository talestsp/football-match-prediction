# football-match-prediction
Repository to work on [Kaggle's Football Match Probability Prediction challenge](https://www.kaggle.com/competitions/football-match-probability-prediction/).

# Project Structure
* data/
  * raw/     <- The original, immutable data dump
  * interim/  <- Intermediate data that has been transformed

* src/
  * dao/
    * dao.py
    * dao_raw.py
    * dao_interim.py
    * columns.py

  * utils/
    * dflib.py
    * plot.py
    * pretties.py
    * stats.py

* notebooks/
  * [DataUnderstanding.ipynb](notebooks/DataUnderstanding.ipynb)
  * [SplitData.ipynb](notebooks/SplitData.ipynb)
  * [TeamMoodAnalysis.ipynb](notebooks/TeamMoodAnalysis.ipynb)

# Data Types
* Schema
  * All data types can be found here: data/schema.json
* Parsing
  * Datetime columns has been loaded as DateType
  * Boolean data types that we originally filled with 1 and 0 were parsed to True and False

# Train, Test and Validation split
The adopted strategy for splitting train and validation datasets can be found at [SplitData.ipynb](notebooks/SplitData.ipynb) notebook. As the test dataset represents a slice made in time, the validation dataset was splitted in the same way.
The 20% most recent data from training was partitioned to validation dataset.

# New Feature: team_mood_diff
The teams' rating for the current match is missing, may ratings from previous matches be useful?
In order to represent this idea, two new features were created: `home_mood_diff` and `away_mood_diff`.

As the term 'rating' is already defined as score of a team for an specific match, the term 'mood' represents the difference between a team's rating and its opponents' rating in each historical matches (at most 10 matches). The `home_history_mood_n` and `away_history_mood_n` represent it, where 'n' represents the the n historical match.

Once this team_history_mood_n was built, then the team_history_mood_mean was calculated. It is the mean of these previous matches. It was stored at `home_history_mood_mean` and `away_history_mood_mean`.

Finally, both `home_mood_diff` and `away_mood_diff` were created.
  * `home_mood_diff` is `home_history_mood_mean` - `away_history_mood_mean`
  * `away_mood_diff` is `away_history_mood_mean` - `home_history_mood_mean`

Yes, it doesn't need to use both to fit the model, picking one of them is enough since each one synthesizes both sides.

Please, check out the [TeamMoodAnalysis.ipynb](notebooks/TeamMoodAnalysis.ipynb) notebook for the evaluaation of this potential good feature.
