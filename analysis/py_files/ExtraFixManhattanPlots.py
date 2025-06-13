import pandas as pd
import numpy as np
import argparse 
from plotnine import *
from scipy.stats import beta

# update manhattan plots by changing ylim

ukbb_data_dir = '/gpfs/commons/datasets/controlled/ukbb-gursoylab'
parser = argparse.ArgumentParser(description='Process parameters.')
parser.add_argument('--ukbb_data_dir', type=str, help='ukbb data directory',default='')
parser.add_argument('--analysis_output_dir', type=str, help='analysis output directory (in aou-atlas-phenotyping folder)',default='')
args = parser.parse_args()


# alter make_manhattan_plot defn
def make_manhattan_plot_ylim(plink_results, output_path, cohortId,db, ylim):
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
    assert plink_results['minuslog10p'].max() < ylim
    plot = (ggplot(plink_results) 
    + geom_point(aes(x='adjusted_POS', y='minuslog10p', colour='factor(color_group)'), alpha=0.5,size=1)  # Plot points colored by chromosome
    + scale_color_manual(values=["#6F8FAF", "#00008B"],guide=False)
    + labs(x='Chromosome', y='-log10(p-value)')  
    + theme(panel_grid=element_blank(),panel_border=element_blank(),legend_position=None, panel_background=element_rect(fill='white'),axis_text_x=element_text(size=6),figure_size=(8,2)) 
    + scale_x_continuous(labels=list(chromosome_mid.keys()), breaks=list(chromosome_mid.values()))
    + geom_hline(yintercept=-np.log10(5e-8), colour='grey',linetype='dashed', color='gray')
    + coord_cartesian(ylim=(0, ylim))

    )
    plot.save(f'{output_path}/Manhattan_Plot_c{cohortId}_{db}.jpeg', dpi=300)

# make qq plot
def make_qq_plot(plink_results, output_path, cohortId,db):
    plink_results = plink_results[plink_results['TEST']=='ADD'].copy()
    
    expected_p_values = np.linspace(1/(len(plink_results['P'])+1), 1, len(plink_results['P']))
    # generate ci
    ci = 0.95
    ci_lower = beta.ppf((1-ci)/2, range(1, len(plink_results['P']) + 1),range(len(plink_results['P']), 0, -1))
    ci_upper = beta.ppf((1+ci)/2, range(1, len(plink_results['P']) + 1),range(len(plink_results['P']), 0, -1))

    # generate ci
    observed_p_values = np.sort(plink_results['P'])
    q_q_data = pd.DataFrame({
    'Expected': -np.log10(expected_p_values),
    'CI lower': -np.log10(ci_lower),
    'CI upper': -np.log10(ci_upper),
    'Observed': -np.log10(observed_p_values)
        })
    qq_plot = (
    ggplot(q_q_data, aes(x='Expected', y='Observed')) +
    geom_point(color='black') +
    geom_ribbon(aes(ymin='CI lower', ymax='CI upper'), fill='gray', alpha=0.2) +
    geom_abline(slope = 1, intercept = 0, color = 'red') +
    labs(x='Expected -log10(p-value)', y='Observed -log10(p-value)') +
    theme(panel_grid=element_blank(),panel_border=element_blank(),legend_position=None, panel_background=element_rect(fill='white'),axis_text_x=element_text(size=6)) 

    )
    qq_plot.save(f'{output_path}/qq_Plot_c{cohortId}_{db}.jpeg', dpi=300)

# once job is finished, create manhattan plot
ylim_dict = {28:30, 288:225, 71:45}
for cohortId in [28,288,71]:
    plink_results = pd.read_csv(f'{args.ukbb_data_dir}/Atlas2AoU/PHENO_{cohortId}/RESULTS_FILE.Phenotype.glm.logistic.hybrid',sep='\t')
    assert plink_results['ERRCODE'].unique().tolist() == ['.']
    #make_manhattan_plot_ylim(plink_results,args.analysis_output_dir,cohortId,'ukbb',ylim_dict[cohortId])
    make_qq_plot(plink_results,args.analysis_output_dir,cohortId,'ukbb')