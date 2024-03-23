# helper functions to make identical graphs in UKBB and AoU
import os
import pandas as pd
from io import StringIO
from statsmodels.stats.proportion import proportions_ztest
from scipy.cluster.hierarchy import linkage, dendrogram
import numpy as np
from plotnine import *
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

def get_co_prev_df(cohorts,N,prev,db):
    # takes cohort_dict where keys are cohort ids and values are set of patient ids; N is total # in OMOP Person table
    # calculate coprevalence as the proportion of patients exhibiting a doublet of conditions
    # https://bmjopen.bmj.com/content/bmjopen/9/10/e031281.full.pdf
    # only perform analyses where prevalence > 20, only return co-prev if count is greater than 20
    cohorts = {k: v for k, v in cohorts.items() if len(v) > 20}
    coprev = pd.DataFrame(0.0, index=cohorts.keys(), columns=cohorts.keys())

    for i in cohorts.keys():
        for j in cohorts.keys():
            # should just be equal to prevalence if i == j
            if i == j:
                assert len(cohorts[i].intersection(cohorts[j])) == prev[prev['cohortId']==i][f'Count_{db}'].values[0]
                assert len(cohorts[i].intersection(cohorts[j])) > 20
                coprev.loc[i,j] = len(cohorts[i].intersection(cohorts[j]))/N
            else:
                if len(cohorts[i].intersection(cohorts[j])) > 20:
                    coprev.loc[i,j] = len(cohorts[i].intersection(cohorts[j]))/N
                else:
                    coprev.loc[i,j] = np.nan
    return coprev

def get_choropleth_map(merged,cases,region_column,person_column,regions,output_path,cohortId,us=False):
    # get total # of individuals by region
    counts_total = merged.groupby(region_column).size().reset_index(name='total_count')

    # subset merged to cases
    merged = merged[merged[person_column].isin(cases)].copy()
    # aggregate counts by region
    merged_counts = merged.groupby(region_column).size().reset_index(name='count')
    # for counts less than 20, set to 0
    merged_counts['count'] = merged_counts['count'].apply(lambda x: 0 if x <= 20 else x)
    merged_counts = merged_counts.merge(counts_total,how='left',on=region_column)
    mask = merged_counts.isna().any(axis=1)
    assert merged_counts[mask].empty
    # only calculate prev where count >20 and total count > 41 otherwise set to 0
    merged_counts['prev'] = merged_counts.apply(lambda row: row['count']/row['total_count'] if ((row['count']>20)&(row['total_count']>41)) else 0,axis=1)
    mask = merged_counts.isna().any(axis=1)
    assert merged_counts[mask].empty

    # merge back with region data
    regions_with_counts = regions.merge(merged_counts, how='left', on=region_column)
    # set na to prev 0 as well
    regions_with_counts['prev'] = regions_with_counts['prev'].fillna(0)

    # Plot
    fig, ax = plt.subplots(1, 1)
    regions_with_counts.plot(column='prev', ax=ax, legend=True,
                            legend_kwds={'label': "Prevalence"},
                            cmap='magma')  
    if us == True:
        plt.ylim(25, 60) 
        plt.xlim(-125, -65)
        for spine in ax.spines.values():
            spine.set_visible(False)
        db='aou'
    else:
        for spine in ax.spines.values():
            spine.set_visible(False)
        db='ukbb'
    ax.set_xticks([])
    ax.set_yticks([])
    fig.set_size_inches(10, 10)
    fig.savefig(f'{output_path}/choropleth_c{cohortId}_{db}.jpeg', dpi=300,bbox_inches='tight') 

    regions_with_counts.sort_values('prev',ascending=False,inplace=True)
    regions_with_counts.set_index(region_column,inplace=True)
    regions_with_counts[[col for col in regions_with_counts.columns if col!='geometry']].head(5).to_csv(f'{output_path}/c{cohortId}_regions_w_highest_prev_{db}.csv')