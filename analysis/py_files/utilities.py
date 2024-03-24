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
from gtfparse import read_gtf

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

def make_manhattan_plot(plink_results, output_path, cohortId,db):
    plink_results = plink_results[plink_results['TEST']=='ADD'].copy()
    plink_results['color_group'] = plink_results['#CHROM'] % 2
    # get chromosome midpoint and adjust pos for graphing
    chromosome_lengths = plink_results.groupby('#CHROM')['POS'].max()
    chromosome_starts = chromosome_lengths.cumsum().shift(1).fillna(0)
    plink_results = plink_results.merge(chromosome_starts.rename('start_pos'), on='#CHROM')
    plink_results['adjusted_POS'] = plink_results['POS'] + plink_results['start_pos']
    chromosome_mid = plink_results.groupby('#CHROM')['adjusted_POS'].apply(lambda x: (x.min() + x.max()) / 2)
    chromosome_mid = chromosome_mid.to_dict()
    plink_results['#CHROM'] = pd.Categorical(plink_results['#CHROM'])
    # -log(p-value)
    plink_results['minuslog10p'] = -np.log10(plink_results['P'])
    plot = (ggplot(plink_results) 
    + geom_point(aes(x='adjusted_POS', y='minuslog10p', colour='factor(color_group)'), alpha=0.5,size=1)  # Plot points colored by chromosome
    + scale_color_manual(values=["#6F8FAF", "#00008B"],guide=False)
    + labs(x='Chromosome', y='-log10(p-value)')  
    + theme(panel_grid=element_blank(),panel_border=element_blank(),legend_position=None, panel_background=element_rect(fill='white'),axis_text_x=element_text(size=6),figure_size=(8,2)) 
    + scale_x_continuous(labels=list(chromosome_mid.keys()), breaks=list(chromosome_mid.values()))
    + geom_hline(yintercept=-np.log10(5e-8), colour='grey',linetype='dashed', color='gray')

    )
    plot.save(f'{output_path}/Manhattan_Plot_c{cohortId}_{db}.jpeg', dpi=300)

# get gene exon annotations
#get gene exons of the snps
def get_gtf(PATH_GTF):
    # gets info on autosomal, protein-coding genes
    gtf = pd.DataFrame()
    gtf= read_gtf(PATH_GTF, 
                usecols=['seqname','gene_id','feature','start',
                        'end', 'gene_type','strand', 'gene_name'])
    # get autosomal chromosome
    gtf = gtf.to_pandas()
    gtf.seqname = gtf['seqname'].apply(lambda x: x.replace('chr', ''))
    gtf = gtf[gtf['seqname'].isin([str(i) for i in range(1,23)])]
    gtf['seqname'] = gtf['seqname'].cat.remove_unused_categories()
    gtf.seqname = gtf.seqname.astype(int)
    # get protein-coding genes
    gtf = gtf[gtf.gene_type=='protein_coding'].copy()
    # can remove rows with feature 'gene' - they are redundant
    gtf = gtf[gtf.feature!='gene'].copy()
    return gtf

def get_gene_annot(results, gtf_path, gtf = None):
    # return dataframe that has additional column with gene list and another with the corresponding features
    if gtf is None:
        gtf = get_gtf(gtf_path)
    if not results.empty:
        #get results in genes
        results[['gene_annot_exon', 'gene_annot_other']] = results.apply(lambda row: pd.Series([
            gtf[(gtf['start'] <= row['POS']) & (gtf['end'] >= row['POS']) & (row['#CHROM'] == gtf['seqname'])&(gtf['feature']=='exon')]['gene_name'].unique().tolist(),
            gtf[(gtf['start'] <= row['POS']) & (gtf['end'] >= row['POS']) & (row['#CHROM'] == gtf['seqname'])&(gtf['feature'].isin(['UTR', 'CDS', 'start_codon', 'stop_codon']))]['gene_name'].unique().tolist()
        ]),axis=1)
        # make sure that only returning unique vals
        assert all(results['gene_annot_exon'].apply(lambda lst: len(lst) == len(set(lst))).values)
        assert all(results['gene_annot_other'].apply(lambda lst: len(lst) == len(set(lst))).values)
    return results