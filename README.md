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
    * dao_processed.py
    * dao_ml.py
    * columns.py

  * ml/
    * estimators_lib/
    * transformers_lib/
      * home_factor.py
      * team_history_result.py
      * team_mood_diff.py
      * fill_proba_transformer.py
    * estimators.py
      * home_factor_estimator.py
      * fill_proba_estimator.py
    * model_selection.py
    * transformers.py

  * utils/
    * dflib.py
    * plot.py
    * plot_domain.py
    * pretties.py
    * stats.py
    * palette.py

* notebooks/
  * [1-DataUnderstanding.ipynb](notebooks/1-DataUnderstanding.ipynb)
  * [2-SplitData.ipynb](notebooks/2-SplitData.ipynb)
  * [3.1-TeamMoodAnalysis.ipynb](notebooks/3.1-TeamMoodAnalysis.ipynb)
  * [3.2-TeamHistoryResultAnalysis.ipynb](notebooks/3.2-TeamHistoryResultAnalysis.ipynb)
  * [3.3-HomeFactorAnalysis.ipynb](notebooks/3.3-HomeFactorAnalysis.ipynb)
  * [4-BuildData.ipynb](notebooks/4-BuildData.ipynb)
  * [Appendix-FillProba.ipynb](notebooks/Appendix-FillProba.ipynb)

# Data Types
### Schema
All data types can be found here: data/schema.json
### Parsing
Datetime columns has been loaded as DateType
Boolean data types that we originally filled with 1 and 0 were parsed to True and False

# Train, Test and Validation split
The adopted strategy for splitting train and validation datasets can be found at [SplitData.ipynb](notebooks/SplitData.ipynb) notebook. As the test dataset represents a slice made in time, the validation dataset was splitted in the same way.
The 20% most recent data from training was partitioned to validation dataset. Then, there are three datasets here: `train_train`, `train_valid` and `test`.


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

It is relevant to be aware that this feature won't be built for `test` dataset because there is no score/target on it. The value, for each league, will be defined from `train` dataset (`train_train` and `train_valid`). In order to safely use it, an equivalence test was performed and analyzed to check whether the factor is steady from `train_train` to `train_valid` if so, we can rely that it would still be usfeul for `test` dataset.

# Transformers
There are three Transformers built under the most relevant analysis made at Jupyter Notebooks at `notebooks/`. They are:

* TeamMoodDiffTransformer
* TeamHistoryResultTransformer
* HomeFactorTransformer (built from HomeFactorEstimator.fit())

Other Transformers were built in order to compose some eventually performed transformations on the ML Pipeline, such as:

* SelectColumnsTransformer
* DropNaTransformer

**Obs**
All the Transformers extend the `MLWritable` and `MLReadable` in order to allow its persistence.

The Transformers are placed at [src/ml_pipeline/transformers.py](src/ml/transformers.py)

# Missing Values
Matches with missing values aren't applied to prediction model.
There are two ways to overcome this problem: 
 * filling missing values on predictors (independent variables)
 * filling missing values on prediction (dependent variable)

Following the holistic strategy of doing simple things first, it will be preffered to start filling missing values on prediction.

### Missing Values on Predictors
<i>Not yet implemented.</i>>

### Missing Values on Prediction
Once the prediction is calculated for all rows (matches) with all valid values, the matches with invalid (null) values also need to have the prediction probability.

To do that, there was designed three stretegies: `uniform_proba`, `global_frequency` and `league_frequency`.

 * `uniform_proba`
   * apply the same probability for all labels: 0.333.

 * `global_frequency`
   * set the global labels frequency as labels probability.

 * `league_frequency`
   * set the league's labels frequency as labels probability.
 
The estimator [`FillProbaEstimator`](src/ml/estimators.py) is the class that calculates the strategy values.
This is achieved through the method `FillProbaEstimator.fit()` that returns a [`FillProbaTransformer`](src/ml/transformers.py) object.




