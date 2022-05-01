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
    * plot_domain.py
    * pretties.py
    * stats.py

* notebooks/
  * [DataUnderstanding.ipynb](notebooks/DataUnderstanding.ipynb)
  * [SplitData.ipynb](notebooks/SplitData.ipynb)
  * [TeamMoodAnalysis.ipynb](notebooks/TeamMoodAnalysis.ipynb)
  * [TeamHistoryResultAnalysis.ipynb](notebooks/TeamHistoryResultAnalysis.ipynb)
  * [HomeFactorAnalysis.ipynb](notebooks/HomeFactorAnalysis.ipynb)

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

# New Feature: team_result_history_mean
This feature summarizes the 10 history matches.
It replaces each team history match with 1, for a victory, with -1 for a defeat and 0 for a draw. Then a mean is calculated for these values for both home and away teams on the match. The checkou [TeamHistoryResultAnalysis.ipynb](notebooks/TeamHistoryResultAnalysis.ipynb) for more information.
The features created are: `home_result_history_mean` and `away_result_history_mean`.

# New Feature: home_factor
Let the frequency of home team victories across the whole league be the `home_factor`.
It seems that some leagues have their peculiarities that make the home factor roughly steady over time.
As an example, the `Copa del Rey` league has the lowest home_factor. It may happen due to its nature. In the first part of the league, the stronger team plays as away team in single match with the weaker team.
Please check [HomeFactorAnalysis.ipynb](notebooks/HomeFactorAnalysis.ipynb) for deatils.

**Obs:** Check whether there is a growth trend in home_factor after the pandemic quarentine. Maybe it has grown when the audience were allowed to go back to stadiums.

# Transformers
There are three transformers built under the most relevant analysis made at Jupyter Notebooks at `notebooks/`. They are:
 * [src/ml_pipeline/transformers_lib/home_factor.py](src/ml_pipeline/transformers_lib/home_factor.py)
 * [src/ml_pipeline/transformers_lib/team_history_result.py](src/ml_pipeline/transformers_lib/team_history_result.py)
 * [src/ml_pipeline/transformers_lib/team_mood_diff.py](src/ml_pipeline/transformers_lib/team_mood_diff.py)