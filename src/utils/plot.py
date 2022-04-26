import matplotlib.pyplot as plt
from matplotlib_venn import venn2
import seaborn as sns
import pandas as pd

import pyspark.sql.functions as f
from src.utils import pretties, dflib

PALETTE = sns.color_palette("tab10").as_hex()
DATASETS_PAL = {'train': PALETTE[0], 'test': PALETTE[1], 'validation': PALETTE[2]}

def bar(df, x, y, legend=None, title=None, x_label=None, y_label=None, color=None, alpha=1.0,
        figsize=(8, 4), plot=True, fig=None):

    pdf = dflib.df_to_dict(df, colnames=[x, y])
    use_x = pdf[x]
    use_y = pdf[y]

    fig = fig if fig else plt.figure(figsize=figsize)

    plt.bar(use_x, use_y, width=5, alpha=alpha, label=legend, color=color)

    plt.xlabel(x_label) if x_label else None
    plt.ylabel(y_label) if y_label else None

    plt.title(title) if title else None
    plt.legend() if legend else None
    plt.show() if plot else None


def bar_multi(df_list, x, y, legend_list=None, title=None, x_label=None, y_label=None,
              color_list=None, alpha=0.7, figsize=(8, 4)):
    fig = plt.figure(figsize=figsize)

    for i in range(len(df_list)):
        df = df_list[i]
        legend = legend_list[i] if legend_list else None
        color = color_list[i] if color_list else None

        bar(df, x, y, legend=legend, x_label=x_label, y_label=y_label, color=color,
            alpha=alpha, figsize=figsize, plot=False, fig=fig)

    plt.title(title) if title else None
    plt.show()

    freq = []
    total = 0
    for i in range(len(df_list)):
        df = df_list[i]
        legend = legend_list[i]
        legend_total = df.agg(f.sum(y).alias("total")).collect()[0].total
        total += legend_total
        freq.append({y: legend_total, "partition": legend})

    freq = pd.DataFrame(freq)
    freq["relative_freq"] = round(freq[y] / total, 4)
    freq = freq.set_index("partition")
    pretties.display(freq)


def venn(df1, df2, on_colnames, labels=None, title=None, colors=('#3c89d0', '#FFB20A'), alpha=0.5, figsize=None):

    on_colnames = [on_colnames] * 2 if not isinstance(on_colnames, list) == 1 else on_colnames
    colname1, colname2 = on_colnames

    label1 = labels[0] if labels else df1.columns[0]
    label2 = labels[1] if labels else df2.columns[0]

    set1 = df1.select([colname1]).distinct()
    set2 = df2.select([colname2]).distinct()
    intersection = set1.intersect(set2)

    len_intersection = intersection.count()
    len_exclusive_set1 = set1.count() - len_intersection
    len_exclusive_set2 = set2.count() - len_intersection

    plt.figure(figsize=figsize)
    plt.title(title)

    venn2(subsets=(len_exclusive_set1, len_exclusive_set2, len_intersection),
                 set_labels=(label1, label2),
                 set_colors=colors,
                 alpha=alpha)
    plt.show()

    total = len_exclusive_set1 + len_exclusive_set2 + len_intersection

    rel_fre = pd.DataFrame({f"{label1} exclusive": [round(len_exclusive_set1 / total, 4)],
                            f"intersection": [round(len_intersection / total, 4)],
                            f"{label2} exclusive": [round(len_exclusive_set2 / total, 4)]
                            }, index=["relative_freq"])

    pretties.display(rel_fre)


def hist(df, colname, title="", ylabel="", bins=30, color=None, figsize=(6, 3)):
    plt.figure(figsize=figsize)
    plt.hist(dflib.df_to_dict(df, colnames=[colname])[colname], density=False, bins=bins, color=color)
    plt.grid(zorder=0)
    plt.xlabel(colname)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.show()


def hist_overlay(df, groupby_colname, histogram_colname, title="", bins=None, alpha=1, figsize=(6, 3)):
    grouped_values = df.groupby(groupby_colname).agg(f.collect_list(histogram_colname).alias(histogram_colname))
    grouped_values_dict = dflib.df_to_dict(grouped_values, grouped_values.columns)

    plt.figure(figsize=figsize)

    for i in range(len(grouped_values_dict.keys())):
        label = grouped_values_dict[groupby_colname][i]
        values = grouped_values_dict[histogram_colname][i]

        plt.hist(values, bins=bins, alpha=alpha, label=label)

    plt.grid(zorder=0)
    plt.xlabel(histogram_colname)
    plt.ylabel("count")
    plt.legend(title="target")
    plt.title(title)
    plt.show()
