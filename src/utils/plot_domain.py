import pyspark.sql.functions as f
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
from pivot.utils import dflib, palette


def plot_home_factor_over_time(df_time, df_total, df_n_matches_time, plot_rankings=[], figsize=(12, 5)):
    window = Window.orderBy('Relative')
    df_total = df_total.withColumn('rank', f.row_number().over(window))

    league_home_n_matches_dt_dict_list = []
    league_name_list = []
    color_list = []
    max_xlim = 0

    plt.figure(figsize=figsize)

    count = 0
    for rank in plot_rankings:
        color = palette.PALETTE[count]
        color_list.append(color)

        league_name = df_total.filter(f.col("rank") == rank).collect()[0].league_name

        league_name_list.append(league_name)
        league_home_freq_league_dt = df_time.filter(f.col("league_name") == league_name)
        league_home_freq_league_dt_dict = dflib.df_to_dict(league_home_freq_league_dt.sort("month"),
                                                           ["month", "Relative"])

        league_home_freq_league_mediana = df_total.filter(f.col("league_name") == league_name).collect()[0][
            "Relative"]
        league_home_n_matches_dt_dict = dflib.df_to_dict(
            df_n_matches_time.filter(f.col("league_name") == league_name), ["month", "n_matches"])
        league_home_n_matches_dt_dict_list.append(league_home_n_matches_dt_dict)

        max_xlim = max(max_xlim, len(league_home_freq_league_dt_dict["Relative"]))
        plt.plot(league_home_freq_league_dt_dict["Relative"], label=league_name, linewidth=2, marker="o", color=color)
        plt.plot(range(-1, 104, 1),
                 [league_home_freq_league_mediana] * 105,
                 linewidth=4, alpha=0.2, color=color)
        count += 1

    plt.xlim(-1, max_xlim)
    plt.ylim(0, 1)
    plt.legend()
    plt.title("HOME FACTOR per month")
    plt.show()

    for i in range(len(league_name_list)):
        fig = plt.figure(figsize=(figsize[0], 1))
        league_name = league_name_list[i]
        league_home_n_matches_dt_dict = league_home_n_matches_dt_dict_list[i]
        color = color_list[i]

        plt.bar(list(range(len(league_home_n_matches_dt_dict["n_matches"]))),
                league_home_n_matches_dt_dict["n_matches"],
                label=league_name, width=0.5, color=color)

        plt.xlim(-1, max_xlim)
        plt.legend()
        plt.title("Total month matches")
        plt.show()
