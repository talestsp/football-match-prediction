# football-match-prediction
Repository to work on [Kaggle's Football Match Probability Prediction challenge](https://www.kaggle.com/competitions/football-match-probability-prediction/).
Most of the work here was made with PySpark.

# Project Structure
* data/
  * raw/     <- The original, immutable data dump
  * interim/  <- Intermediate data that has been transformed
  * processes/ <- Final data (with feature engineering stages applied)
  * preds/ <- Test predictions are stored here
  * feature_selection/ <- Features selected and its correlation with target
  * results/ <- Data from Cross-Validation Grid Search experiments are stored here.

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
      * home_factor_estimator.py
      * fill_proba_estimator.py
    * transformers_lib/
      * team_history_result.py
      * team_mood_diff.py
      * fill_proba_transformer.py
    * estimators.py
    * transformers.py
    * metrics.py
    * missin_values.py

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
  * [4.1-BuildData.ipynb](notebooks/4.1-BuildData.ipynb)
  * [4.2-FeatureSelection.ipynb](notebooks/4.2-FeatureSelection.ipynb)
  * [4.3-MissingValues.ipynb](notebooks/4.3-MissingValues.ipynb)
  * [5-Baselines.ipynb](notebooks/5-Baselines.ipynb)
  * [6.1-ExperimentRandomForest.ipynb](notebooks/6.1-ExperimentRandomForest.ipynb)
  * [6.2-ExperimentXGBoost.ipynb](notebooks/6.2-ExperimentXGBoost.ipynb)
  * [7.1-ResultAnalysis.ipynb](notebooks/7.1-ResultAnalysis.ipynb)
  * [8-BuildBestModelAndPredict.ipynb.ipynb](notebooks/8-BuildBestModelAndPredict.ipynb.ipynb)  
  * [Appendix-FillProba.ipynb](notebooks/Appendix-FillProba.ipynb)

# Data Types
### Schema
All data types can be found here: data/schema.json
### Parsing
Datetime columns has been loaded as DateType
Boolean data types that we originally filled with 1 and 0 were parsed to True and False

# Train, Test and Validation split
The adopted strategy for splitting train and validation datasets can be found at [2-SplitData.ipynb](notebooks/2-SplitData.ipynb) notebook. As the test dataset represents a slice made in time, the validation dataset was splitted in the same way.
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

Please, check out the [3.1-TeamMoodAnalysis.ipynb](notebooks/3.1-TeamMoodAnalysis.ipynb) notebook for the evaluaation of this potential good feature.

# New Feature: team_result_history_mean
This feature summarizes the 10 history matches.
It replaces each team history match with 1, for a victory, with -1 for a defeat and 0 for a draw. Then a mean is calculated for these values for both home and away teams on the match. The checkou [3.2-TeamHistoryResultAnalysis.ipynb](notebooks/3.2-TeamHistoryResultAnalysis.ipynb) for more information.
The features created are: `home_result_history_mean` and `away_result_history_mean`.

# New Feature: home_factor
Let the frequency of home team victories across the whole league be the `home_factor`.
It seems that some leagues have their peculiarities that make the home factor roughly steady over time.
As an example, the `Copa del Rey` league has the lowest home_factor. It may happen due to its nature. In the first part of the league, the stronger team plays as away team in single match with the weaker team.
Please check [3.3-HomeFactorAnalysis.ipynb](notebooks/3.3-HomeFactorAnalysis.ipynb) for deatils.

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

# Build Data
The features construction is applied for `train_train`, `train_valid` and `test` datasets in the [BuildData.ipynb](notebooks/4.1-BuildData.ipynb) notebook using the Transformers commented before. The processed data is stored with an id associated to it.

# Feature Selection
The feature selection was made in two steps:
  * ANOVA's F Statistic between numerical features and target
  * Mutually correlated features analysis
    * For each pair of mutual highly correlated features (threshold above Pearson's 0.9) the one with lower correlation with target was discarded.

  More details of it can be found at [4.2-FeatureSelection.ipynb](notebokos/4.2-FeatureSelection.ipynb)

# Missing Values Analysis
Matches with missing values in features aren't applied to prediction model. It is relevant to assess the quality of the data on this matter.
You can check this summary analysis in [4.3-MissingValues.ipynb](notebooks/4.3-MissingValues.ipynb).

There are two ways to overcome this problem: 
 * filling missing values on predictors (independent variables)
 * filling missing values on prediction (dependent variable)

Following the holistic strategy of doing simple things first, it will be preffered to start with simple strategies.

### Missing Values on Predictors (indepedent variables)
Feature median value imputation was applied to all features for those matches with missing values.
An interesting strategy of filling missing values with the mean of similar matches will be done later.

### Missing Values on Prediction  (depedent variable)
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
More information can be found at [Appendix-FillProba.ipynb](notebooks/Appendix-FillProba.ipynb)

# Baselines
It is a good practice to start a Machine Learning project with one or more scored baseline approaches.
It represent the scores reached by simple strategies with no intelligent effort.
The chosen baseline strategies were the ones presented in <b>Missing Values on Prediction</b> section: `uniform_proba`, `global_frequency` and `league_frequency`.

Check notebook [5-Baselines.ipynb](notebooks/5-Baselines.ipynb) for details.

# Modeling and Inference
Model Selection (also known as Model Tuning) is the process of finding the set of parameters (over a collection of them) that yields the best score.

In this stage the concern is about having a model with a good learning (able to apply relevant identified patterns) and a good generalization (able to perform well on unseen data).

To do this experiment and save the results, the following jupyter notebooks were used. Both notebooks executes Cross-Validation strategy with K-Fold splits under a Grid Search heuristic.

 * [6.1-ExperimentRandomForest.ipynb](notebooks/6.1-ExperimentRandomForest.ipynb)
 * [6.2-ExperimentXGBoost.ipynb](notebooks/6.2-ExperimentXGBoost.ipynb)

The data used on Cross-Validation is the training data with no missing values on feature set. As fitting and validating datasets come from the the same Cross-Validation input dataset, it is not a good idea to let synthetically filled data points compose this input data. They must be excluded from it.

As the `test` dataset has also features with missing values, some strategy to work on it must be used. The notebook 7.2-ModelSelectionAndPrediction is used to experiment the missing values approaches that was mentioned before.

<b>Feature Importances</b>
The relevance of each feature, stated by the best model, is also shown in the Experiments notebook. 

<b>Overfitting Analysis</b>
A table with the train and cross-validation metrics (for each parameter configuration) is also calculated and stored in order to leverage further knowledge/insights for future iterations. A brief overfitting analysis can be found at 7.1-ResultAnalysis.

In the experiment stages, the Cross-Validation was performed in two types of training data, both without missing values. They come from the same `ttrain` dataset, but one of them have all target classes balanced.

It is not fair to compare their Cross-Validation results because the dataset the input dataset is not the same. Then the `tvalid` dataset is used.

The score comparison between models fitted with and withou class balacing is assessed by evaluating predictions over the `tvalid` dataset. The missing values strategy is also evaluated with it.
This final evaluation is performed in the  [7.2-ModelSelectionAndPrediction.ipynb/](notebooks/7.2-ModelSelectionAndPrediction.ipynb) jupyter notebook.

# Prediction
The best model performance, according to the experiment notebooks, are re-built and applied to `test` dataset in the [8-BuildBestModelAndPredict.ipynb](notebooks/8-BuildBestModelAndPredict.ipynb) notebooks.
It also builds the submission file to be evaluated at Kaggle.

# Future Work
This project was made within a month by me, starting with simple approach and then improving.
Unfortunately I started it when the competition was about to finish and I am not in the learderboard.
The score I got with this code is enough to be in the top 50%.
If I had more time to work on it, I would do the following.

  * Missing Values Imputation with Similar Matches.
    * The two most relevant features have around 8.5% os missing values in the `test` dataset. 
    * They were filled with global median values, but they can have a better filling strategy considering similar data points (matches).

  * More Feature Engineering
    * This is one the most relevant effort in Machine Learning. All the used features here were built from the original ones.
    * There are lots of interesting new features that can be tried here. I would begin with coach win rate.
    * Maybe aggregating the current features to the month of the year is a good try. The leagues are seasonal and due to climate Season of the rules of the competitions, some aspects may change.

  * Another Learning Algorithms
    * I have used two simple ones: RandomForest and XGBoost classifiers. Maybe an LSTM or even a LGBM would be improve score.
    * Stack Ensembles is also agood try.

  * The overfitting analysis can be found in the notebook [7.1-ResultAnalysis.ipynb](notebooks/7.1-ResultAnalysis.ipynb) and can leverage new insights to add on Grid Search parameters.

So, these are the efforts that I would have done if I had more time :)

Thank you 




