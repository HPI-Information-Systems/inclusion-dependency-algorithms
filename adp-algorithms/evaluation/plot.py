#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import os.path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter


algorithms = ['bellbrockhausen', 'demarchi', 'spider', 'spider-bf', 'sindd', 'binder']
algorithm_names = ['BellBrockhausen', 'DeMarchi', 'Spider', 'Spider BF', 'S-INDD', 'BINDER']

# Colors from the default property cycle
# https://matplotlib.org/gallery/color/color_cycle_default.html
algorithm_colors = plt.rcParams['axes.prop_cycle'].by_key()['color'][:len(algorithms)]
algorithm_to_color = dict(zip(algorithms, algorithm_colors))

# https://matplotlib.org/api/markers_api.html#module-matplotlib.markers
algorithm_markers = ['^', 'D', 'v', 'P', 'o',    's']
algorithm_to_marker = dict(zip(algorithms, algorithm_markers))


def read_dataset(name):
    raw = pd.read_csv(os.path.join('parsed-output', name),
                      header=None,
                      index_col=0)
    return raw.T.reindex(columns=algorithms)


scop = read_dataset('scop.csv')
biosql = read_dataset('biosql.csv')
cath = read_dataset('cath.csv')
tesma = read_dataset('tesma.csv')
musicbrainz = read_dataset('musicbrainz.csv')
wikirank = read_dataset('wikirank.csv')
wikipedia = read_dataset('wikipedia.csv')
lod = read_dataset('lod.csv')
ensembl = read_dataset('ensembl.csv')
census = read_dataset('census.csv')
tpch1 = read_dataset('tpch1s.csv')
tpch10 = read_dataset('tpch10s.csv')

#datasets = ['scop', 'biosql', 'cath', 'tesma', 'musicbrainz', 'wikirank', 'wikipedia', 'lod', 'ensembl', 'census', 'tpc-h 1', 'tpc-h 10']
datasets =  ['scop', 'census', 'wikipedia', 'biosql', 'wikirank', 'lod', 'cath', 'ensembl', 'tesma', 'tpc-h 1', 'tpc-h 10', 'musicbrainz']
dataset_list =  [scop, census, wikipedia, biosql, wikirank, lod, cath, ensembl, tesma, tpch1, tpch10, musicbrainz]
#dataset_list = [scop, biosql, cath, tesma, musicbrainz, wikirank, wikipedia, lod, ensembl, census, tpch1, tpch10]
dataset_names = [d.upper() for d in datasets]
dataset_by_name = dict(zip(datasets, dataset_list))
dataset_means = {name: dataset.mean() for (name, dataset) in dataset_by_name.items()}


combined = pd.concat(dataset_means, axis=1).reindex(columns=datasets)

sindd_partitions_scop = pd.read_csv("dataset-runtime-results/sindd-partition-scop.csv", header=None)
sindd_partitions_cath = pd.read_csv("dataset-runtime-results/sindd-partition-cath.csv", header=None)
sindd_partitions_biosql = pd.read_csv("dataset-runtime-results/sindd-partition-biosql.csv", header=None)

rowcount = pd.read_csv("parsed-output/rowcount.csv", header=None, index_col=0).T.reindex(columns=algorithms)
rowcount3 = pd.read_csv("parsed-output/rowcount-musicbrainz.csv", header=None, index_col=0).T.reindex(columns=algorithms)
columncount = pd.read_csv("parsed-output/columncount-cath-tesmaexp.csv", header=None, index_col=0).T.reindex(columns=algorithms) 
columncount2 = pd.read_csv("parsed-output/columncount-pdb-refine.csv", header=None, index_col=0).T.reindex(columns=algorithms)
columncount3 = pd.read_csv("parsed-output/columncount-musicbrainz-editor-sanitised.csv", header=None, index_col=0).T.reindex(columns=algorithms)
distinct = pd.read_csv("dataset-runtime-results/distinct_values.csv", header=None, index_col=0).T.reindex(columns=['demarchi', 'spider'])

DEBUG = False
        
def setup_axes(unit=None, width=8, height=4, dpi=800):
    #plt.style.use(['fast'])
    if not DEBUG:
        plt.gcf().set_size_inches(width, height)
        plt.gcf().set_dpi(dpi)
    #formatter = FuncFormatter(lambda x, p: '{:,}'.format(x))
    
    unit_to_factor = {'seconds': 1000, 'minutes': 1000 * 60, 'percent': 1.0/100}
    if unit:
        factor = unit_to_factor[unit]
        formatter = FuncFormatter(lambda x, p: str(int(x / factor)))
        plt.gca().yaxis.set_major_formatter(formatter)
    
    plt.gca().yaxis.grid()
    plt.gca().set_axisbelow(True) # draw grid below lines / bars


def export(name):    
    if DEBUG:
        plt.show()
        plt.clf()
        return
    
    path = 'export'
    if not os.path.exists(path):
        os.mkdir(path)
        
    
    
    plt.savefig('%s/%s.pdf' % (path, name),
                format='pdf',
                bbox_inches='tight')
    """
    plt.savefig('%s/%s.png' % (path, name),
                format='png',
                bbox_inches='tight')
    """
    plt.clf()


#%%

def plot_algorithm(algorithm_name, algorithm):
    global combined
    setup_axes(unit='minutes')
    plt.title('{}: Algorithm Runtime'.format(algorithm_name))
    plt.xlabel('Dataset')
    plt.ylabel('Runtime (m)')
    
    data = combined.transpose()[algorithm]
    x = np.arange(len(datasets))
    plt.xticks(x, dataset_names)
    plt.bar(x, data, width=.2)
    

    export(algorithm)


#for name, algorithm in zip(algorithm_names, algorithms):
#    plot_algorithm(name, algorithm)

#%%

def plot_dataset(dataset_name, dataset):
    global combined
    setup_axes(unit='minutes')
    plt.title('{}: Algorithm Runtime'.format(dataset_name))
    plt.xlabel('Algorithm')
    plt.ylabel('Runtime (m)')
    
    data = combined[dataset]
    x = np.arange(len(algorithms))
    plt.xticks(x, algorithm_names)
    plt.bar(x, data, width=.2)
    
    export(dataset)


#for name, dataset in zip(dataset_names, datasets):
#    plot_dataset(name, dataset)
 
#%%

# Output table for paper
def csv_algo_runtime_all():
    global combined
    in_minutes = combined.T / 1000 / 60
    rounded = np.round(in_minutes, decimals=2)
    path = 'export'
    if not os.path.exists(path):
        os.mkdir('export')
    rounded.to_csv('export/algorithm_runtime.csv')

csv_algo_runtime_all()

#%%
    
def plot_combined_per_dataset():
    global combined    
    data = combined.transpose()
    m = data.max(axis=1)
    data = data.divide(m, axis=0)
    
   #data.fillna(1, inplace=True)

    setup_axes(unit='percent', width=16, height=4, dpi=1600)
    plt.title('Runtimes')
    plt.xlabel('Dataset')
    plt.ylabel('Runtime in % of longest run')
    
    ax = plt.subplot(111)
    
    bars = []
    w = .1
    pad = .2
    x = np.arange(0,
                  len(datasets) * w * len(algorithms) + len(datasets) * pad, # total width
                  w * len(algorithms) + pad)
    
    x = x[:-1]
    x2 = x - (w * len(algorithms)) / 2
    
    for index, algo in enumerate(data.columns):
        bar = ax.bar(x2 + w * index, data[algo], width=w, align='center')
        bars.append(bar)
        
    plt.legend(bars,
               algorithm_names,
               loc='lower center',
               ncol=len(algorithms),
               bbox_to_anchor=(0.5, -0.2))
    
    plt.xticks(x, dataset_names)#, rotation=45)
    
    export('combined_per_dataset')


plot_combined_per_dataset()

#%%

def plot_combined_per_dataset_slowdown():
    global combined    
    data = combined.transpose()
    #m = data.max(axis=1)
    m = data.min(axis=1)
    data = data.divide(m, axis=0)
    
    #data.fillna(1, inplace=True)

    setup_axes(width=16, height=4, dpi=1600)
    plt.title('Algorithm Runtime per Dataset')
    #plt.xlabel('Dataset')
    plt.ylabel('Slowdown factor compared to shortest runtime')
    
    ax = plt.subplot(111)
    
    bars = []
    w = .1
    pad = .2
    x = np.arange(0,
                  len(datasets) * w * len(algorithms) + len(datasets) * pad, # total width
                  w * len(algorithms) + pad)
    
    x = x[:-1]
    x2 = x - (w * len(algorithms)) / 2
    
    for index, algo in enumerate(data.columns):
        bar = ax.bar(x2 + w * index, data[algo], width=w, align='center')
        bars.append(bar)
        
    plt.legend(bars,
               algorithm_names,
               loc='lower center',
               ncol=len(algorithms),
               bbox_to_anchor=(0.5, -0.2))
    
    plt.xticks(x, dataset_names)#, rotation=45)
    plt.ylim(ymin=0, ymax=10)
    locs, labels = plt.yticks()
    nl = list(np.append(locs, 1))
    nl.sort()
    plt.yticks(nl)

    export('combined_per_dataset_slowdown')


plot_combined_per_dataset_slowdown()

#%%

def plot_combined_per_dataset_log():
    global combined    
    data = combined.transpose()
    #m = data.max(axis=1)
    #m = data.min(axis=1)
    #data = data.divide(m, axis=0)
    
    #data.fillna(1, inplace=True)

    setup_axes(width=16, height=4, dpi=1600)
    plt.title('Runtimes')
    plt.xlabel('Dataset')
    plt.ylabel('Logarithmic Plot of Algorithm Runtime')
    
    ax = plt.subplot(111)
    
    bars = []
    w = .1
    pad = .2
    x = np.arange(0,
                  len(datasets) * w * len(algorithms) + len(datasets) * pad, # total width
                  w * len(algorithms) + pad)
    
    x = x[:-1]
    x2 = x - (w * len(algorithms)) / 2
    #plt.yscale('log')
    
    for index, algo in enumerate(data.columns):
        bar = ax.bar(x2 + w * index, data[algo], width=w, align='center', log=True)
        bars.append(bar)
        
    plt.legend(bars,
               algorithm_names,
               loc='lower center',
               ncol=len(algorithms),
               bbox_to_anchor=(0.5, -0.2))
    
    plt.xticks(x, dataset_names)#, rotation=45)
    
    #plt.ylim(ymin=0, ymax=10)
    #locs, labels = plt.yticks()
    #nl = list(np.append(locs, 1))
    #nl.sort()
    #plt.yticks(nl)
    #print(plt.yticks())
    
    export('combined_per_dataset_log')


plot_combined_per_dataset_log()

#%%

def plot_combined_per_algorithm():
    global combined
    
    setup_axes(unit='seconds')
    plt.title('Runtimes')
    plt.xlabel('Algorithm')
    plt.ylabel('Runtime (s)')
    
    x = np.arange(len(algorithms))
    
    ax = plt.subplot(111)
    b0 = ax.bar(x - 0.3, combined['scop'], width=.2)
    b1 = ax.bar(x + 0.1, combined['biosql'], width=.2)
    b2 = ax.bar(x - 0.1, combined['cath'], width=.2)
    #b3 = ax.bar(x + 0.3, combined['coma'], width=.2)
    b3 = ax.bar(x + 0.3, combined['tesma'], width=.2)
    
    plt.xticks(x, algorithm_names)
    plt.legend([b0, b1, b2, b3],
               dataset_names)
    
    export('combined_per_algorithm')


plot_combined_per_algorithm()
#%%

def plot_sindd_partitions():
    global sindd_partitions_scop, sindd_partititions_cath, sindd_paritions_biosql
    
    def _plot_single(title, export_name, frame):
        setup_axes(unit='seconds')
        plt.title(title)
        plt.xlabel('Partition Count')
        plt.ylabel('Runtime (s)')
        
        plt.plot(frame[0], frame[1])
        plt.ylim(ymin=0)
        export(export_name)
        
    _plot_single('S-INDD Partition Performance: SCOP',
                 'sindd_partition_performance_scop',
                 sindd_partitions_scop)
    
    _plot_single('S-INDD Partition Performance: CATH',
                 'sindd_partition_performance_cath',
                 sindd_partitions_cath)
    
    _plot_single('S-INDD Partition Performance: BIOSQL',
                 'sindd_partition_performance_biosql',
                 sindd_partitions_biosql)


plot_sindd_partitions()
#%%

def plot_rowcount(export_name, algorithms, algorithm_names):
    global rowcount
    
    setup_axes(unit='seconds')
    plt.title('Rowcount Experiment')
    plt.xlabel('Rowcount')
    plt.ylabel('Runtime (s)')
    
    plt.ylim(ymin=1, ymax=550 * 1000)
    
    counts = np.arange(25000, 300000, 25000)
    ref = []
    for col in algorithms:
        ax = plt.plot(counts,
                      rowcount[col],
                      marker=algorithm_to_marker[col],
                      color=algorithm_to_color[col])
        ref.append(ax[0])
    
    plt.legend(ref, algorithm_names)
    
    plt.annotate(" ",
                 xy=(60000, 550 * 1000),
                 xytext=(51_000, 440 * 1000),
                 arrowprops=dict(arrowstyle="->", color='#1f77b4'))
    
    export(export_name)


plot_rowcount('rowcount_all', algorithms, algorithm_names)

#%%

def plot_rowcount_musicbrainz(export_name, algorithms, algorithm_names):
    global rowcount3
    
    setup_axes(unit='minutes')
    
    plt.title('Rowcount Experiment')
    plt.xlabel('Rowcount')
    plt.ylabel('Runtime (m)')
    
    plt.ylim(ymax=12 * 1000 * 60)
    counts = np.arange(1245661, 21 * 1245661, 1245661)
    ref = []
    for col in algorithms:
        ax = plt.plot(counts,
                      rowcount3[col],
                      marker=algorithm_to_marker[col],
                      color=algorithm_to_color[col])
        ref.append(ax[0])
    
    plt.legend(ref, algorithm_names)
    
    plt.text(0.68 * 10**7, 340 * 1000, "$\dagger$", fontsize=14)  # BB
    
    export(export_name)


plot_rowcount_musicbrainz('rowcount_musicbrainz_editor_sanitised_all', algorithms, algorithm_names)

#%%

def plot_columncount(export_name, algorithms, algorithm_names):
    global columncount
    
    setup_axes(unit='seconds')        
    plt.title('Columncount Experiment')
    plt.xlabel('Columncount')
    plt.ylabel('Runtime (s)')

    plt.ylim(ymin=1, ymax=25 * 1000)    
    counts = np.arange(18, 108, 18)
    ref = []
    for col in algorithms:
        ax = plt.plot(counts,
                      columncount[col],
                      marker=algorithm_to_marker[col],
                      color=algorithm_to_color[col])
        ref.append(ax[0])
    
    plt.legend(ref, algorithm_names)
    plt.annotate(" ",
                 xy=(57, 25 * 1000),
                 xytext=(55.5, 20 * 1000),
                 arrowprops=dict(arrowstyle="->", color='#1f77b4'))
    

    export(export_name)


plot_columncount('columncount_all', algorithms, algorithm_names)

#%%

def plot_columncount_pdb_refine(export_name, algorithms, algorithm_names):
    global columncount2
    
    setup_axes(unit='seconds')
    plt.title('Columncount Experiment')
    plt.xlabel('Columncount')
    plt.ylabel('Runtime (s)')
    
    counts = np.arange(8, 88 + 8, 8)
    ref = []
    for col in algorithms:
        ax = plt.plot(counts,
                      columncount2[col],
                      marker=algorithm_to_marker[col],
                      color=algorithm_to_color[col])
        ref.append(ax[0])
    
    
    ind_counts = pd.read_csv('unary-columncount-results-pdb-refine/ind_counts.csv', header=None).T[0]
    runtime_ax = plt.gca()
    ind_ax = plt.gca().twinx()
    ind_plot = ind_ax.plot(counts,
                           ind_counts,
                           marker='*',
                           linestyle=':')
    plt.ylabel('Unary INDs')
    
    ref.append(ind_plot[0])
    plt.legend(ref, algorithm_names + ['IND Count'])

    runtime_ax.set_ylim(ymin=1, ymax=5 * 1000)
    
    runtime_ax.annotate(" ",
         xy=(45, 5 * 1000),
         xytext=(43, 4.2 * 1000),
         arrowprops=dict(arrowstyle="->", color='#1f77b4'))
                             
                             
    export(export_name)


plot_columncount_pdb_refine('columncount_pdb_refine_all', algorithms, algorithm_names)

#%%

def plot_columncount_musicbrainz(export_name, algorithms, algorithm_names):
    global columncount3
    
    setup_axes(unit='minutes')    
    plt.title('Columncount Experiment')
    plt.xlabel('Columncount')
    plt.ylabel('Runtime (m)')
    
    counts = np.arange(20, 400 + 20, 20)
    ref = []
    for col in algorithms:
        ax = plt.plot(counts,
                      columncount3[col],
                      marker=algorithm_to_marker[col],
                      color=algorithm_to_color[col])
        ref.append(ax[0])
    
    ind_counts = pd.read_csv('unary-columncount-results-musicbrainz-editor-sanitised/ind_counts.csv', header=None).T[0]
    runtime_ax = plt.gca()
    
    ind_ax = plt.gca().twinx()
    ind_ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: "{:,}".format(int(x))))
    ind_plot = ind_ax.plot(counts,
                           ind_counts,
                           marker='*',
                           linestyle=':')
    plt.ylabel('Unary INDs')
    
    ref.append(ind_plot[0])
    plt.legend(ref, algorithm_names + ['IND Count'])
    
    runtime_ax.text(270, 10_000 * 1000, "$\dagger$", fontsize=14)  # BB
    #runtime_ax.text(365, 3_200 * 1000, "$\dagger$", fontsize=14)  # SINDD
                   
    export(export_name)


plot_columncount_musicbrainz('columncount_musicbrainz_editor_sanitised_all', algorithms, algorithm_names)

#%%

# Quick and dirty combined plot of musicbrainz within one plot

def plot_musicbrainz_combined(export_name, algorithms, algorithm_names):
    global columncount3
    
    plt.subplot(122)
    
    setup_axes(unit='minutes')    
    #plt.title('Columncount Experiment')
    plt.xlabel('Columncount')
    plt.ylabel('Runtime (m)')
    
    counts = np.arange(20, 400 + 20, 20)
    ref = []
    for col in algorithms:
        ax = plt.plot(counts,
                      columncount3[col],
                      marker=algorithm_to_marker[col],
                      color=algorithm_to_color[col])
        ref.append(ax[0])
    
    ind_counts = pd.read_csv('unary-columncount-results-musicbrainz-editor-sanitised/ind_counts.csv', header=None).T[0]
    runtime_ax = plt.gca()
    
    ind_ax = plt.gca().twinx()
    ind_ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: "{:,}".format(int(x))))
    ind_plot = ind_ax.plot(counts,
                           ind_counts,
                           marker='*',
                           linestyle=':')
    plt.ylabel('Unary INDs')
    
    ref.append(ind_plot[0])
    #plt.legend(ref, algorithm_names + ['IND Count'])
    
    runtime_ax.text(270, 10_000 * 1000, "$\dagger$", fontsize=14)  # BB
    runtime_ax.text(365, 3_200 * 1000, "$\dagger$", fontsize=14)  # SINDD
    
    plt.subplot(121)
    
    global rowcount3
    
    setup_axes(unit='minutes', width=10, height=4)
    
    #plt.title('Rowcount Experiment')
    plt.xlabel('Rowcount')
    plt.ylabel('Runtime (m)')
    
    plt.ylim(ymax=12 * 1000 * 60)
    counts = np.arange(1245661, 21 * 1245661, 1245661)
    #ref = []
    for col in algorithms:
        ax = plt.plot(counts,
                      rowcount3[col],
                      marker=algorithm_to_marker[col],
                      color=algorithm_to_color[col])
        #ref.append(ax[0])
    
    plt.legend(ref, algorithm_names  + ['IND Count'],
               loc='lower center',
               frameon=False,
               ncol=4,
               bbox_to_anchor=(1, -0.3))
    
    plt.text(0.68 * 10**7, 340 * 1000, "$\dagger$", fontsize=14)  # BB

    plt.tight_layout()

    export(export_name)

plot_musicbrainz_combined('musicbrainz_combined', algorithms, algorithm_names)
    

#%%

def plot_distinct():
    global distinct
    
    setup_axes()
    plt.title('Distinctness Experiment')
    plt.xlabel('#Distinct Values')
    plt.ylabel('Runtime (ms)')
    
    counts = np.arange(1000, 71000, 1000)
    ref = []
    for col in distinct.columns:
        ax = plt.plot(counts, distinct[col])
        ref.append(ax[0])
    
    plt.legend(ref, ['DeMarchi', 'SPIDER'])
    
    plt.ylim(ymin=0)
    export('distinct')


plot_distinct()
#%%

# hackedihack
def plot_runtime_shares():
    raw_data = {'spider': [28633, 5090],
                'sindd': [69070, 11544],
                'oldspider': [71683, 3643]}
    
    data = pd.DataFrame(raw_data).reindex(columns=['spider', 'sindd', 'oldspider']).transpose()
    
    setup_axes(unit='seconds')
    plt.title('Runtime Contingents on BIOSQL')
    plt.xlabel('Algorithm')
    plt.ylabel('Runtime (s)')
    
    x = np.arange(len(data))
    b0 = plt.bar(x, data[0])
    b1 = plt.bar(x, data[1], bottom=data[0])
    
    plt.xticks(x, ['Spider', 'S-INDD', 'OldSpider'])
    plt.legend((b0, b1), ('Attribute Creation', 'Discovering INDs'))
    export('runtime_contingents')


plot_runtime_shares()
#%%

def plot_query_runtime():
    raw_data = {'not_in': [50],
                'not_exists': [170],
                'left_outer_join': [55]}
    
    data = pd.DataFrame(raw_data).reindex(columns=['not_in', 'not_exists', 'left_outer_join']).transpose()
    
    setup_axes()
    plt.title('Query Runtimes')
    plt.xlabel('Query')
    plt.ylabel('Runtime (s)')
    
    x = np.arange(len(data))
    plt.bar(x, data[0])
    
    plt.xticks(x, ['NOT IN', 'NOT EXISTS', 'LEFT OUTER JOIN'])
    export('query_runtimes')


plot_query_runtime()
    