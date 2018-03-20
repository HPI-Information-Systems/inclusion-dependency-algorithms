#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import os.path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter


algorithms = ['demarchi', 'spider', 'sindd', 'bb', 'binder']
algorithm_names = ['DeMarchi', 'Spider', 'S-INDD', 'BellBrockhausen', 'BINDER']
datasets = ['scop', 'biosql', 'cath', 'tesma']
dataset_names = [d.upper() for d in datasets]


scop = pd.read_csv('scop.csv', header=None, index_col=0).T.reindex(columns=algorithms)
biosql = pd.read_csv('biosql.csv', header=None, index_col=0).T.reindex(columns=algorithms)
cath = pd.read_csv('cath.csv', header=None, index_col=0).T.reindex(columns=algorithms)
#coma = pd.read_csv('coma.csv', header=None, index_col=0).T.reindex(columns=algorithms)
tesma = pd.read_csv('tesma.csv', header=None, index_col=0).T.reindex(columns=algorithms)


dataset_means = {'scop': scop.mean(),
                 'biosql': biosql.mean(),
                 'cath': cath.mean(),
                 #'coma': coma.mean(),
                 'tesma': tesma.mean()}


combined = pd.concat(dataset_means, axis=1).reindex(columns=datasets)

sindd_partitions_scop = pd.read_csv("sindd-partition-scop.csv", header=None)
sindd_partitions_cath = pd.read_csv("sindd-partition-cath.csv", header=None)
sindd_partitions_biosql = pd.read_csv("sindd-partition-biosql.csv", header=None)

rowcount = pd.read_csv("rowcount.csv", header=None, index_col=0).T
columncount = pd.read_csv("columncount.csv", header=None, index_col=0).T

distinct = pd.read_csv("distinct_values.csv", header=None, index_col=0).T.reindex(columns=['demarchi', 'spider'])

DEBUG = True


def setup_axes(use_seconds=True):
    #plt.style.use(['fast'])
    if not DEBUG:
        plt.gcf().set_size_inches(8, 4)
        plt.gcf().set_dpi(800)
    #formatter = FuncFormatter(lambda x, p: '{:,}'.format(x))
    if use_seconds:
        formatter = FuncFormatter(lambda x, p: str(int(x / 1000)))
        plt.gca().yaxis.set_major_formatter(formatter)


def export(name):
    path = 'export'
    if not os.path.exists(path):
        os.mkdir(path)
    plt.savefig('%s/%s.png' % (path, name), format='png')
    
    if DEBUG:
        plt.show()
    plt.clf()


def plot_algorithm(algorithm_name, algorithm):
    global combined
    setup_axes()
    plt.title('{}: Algorithm Runtime'.format(algorithm_name))
    plt.xlabel('Dataset')
    plt.ylabel('Runtime (ms)')
    
    data = combined.transpose()[algorithm]
    x = np.arange(len(datasets))
    plt.xticks(x, dataset_names)
    plt.bar(x, data, width=.2)
    

    export(algorithm)


def plot_dataset(dataset_name, dataset):
    global combined
    setup_axes()
    plt.title('{}: Algorithm Runtime'.format(dataset_name))
    plt.xlabel('Algorithm')
    plt.ylabel('Runtime (ms)')
    
    data = combined[dataset]
    x = np.arange(len(algorithms))
    plt.xticks(x, algorithm_names)
    print(x)
    print(data)
    plt.bar(x, data, width=.2)
    
    export(dataset)
 
    
def plot_combined_per_dataset():
    global combined    
    data = combined.transpose()

    setup_axes()
    plt.title('Runtimes')
    plt.xlabel('Dataset')
    plt.ylabel('Runtime (s)')
    
    x = np.arange(len(datasets))
    
    ax = plt.subplot(111)
    b0 = ax.bar(x - 0.4, data['demarchi'], width=.2)
    b1 = ax.bar(x - 0.2, data['spider'], width=.2)
    b2 = ax.bar(x + 0, data['sindd'], width=.2)
    b3 = ax.bar(x + 0.2, data['bb'], width=.2)
    b4 = ax.bar(x + 0.4, data['binder'], width=.2)
    
    plt.xticks(x, dataset_names)
    plt.legend([b0, b1, b2, b3, b4],
               algorithm_names)
    
    export('combined_per_dataset')


def plot_combined_per_algorithm():
    global combined
    
    setup_axes()
    plt.title('Runtimes')
    plt.xlabel('Algorithm')
    plt.ylabel('Runtime (ms)')
    
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


def plot_sindd_partitions():
    global sindd_partitions_scop, sindd_partititions_cath, sindd_paritions_biosql
    
    def _plot_single(title, export_name, frame):
        setup_axes()
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
    

def plot_rowcount_all():
    global rowcount
    
    setup_axes()
    plt.title('Rowcount Experiment')
    plt.xlabel('Rowcount')
    plt.ylabel('Runtime (s)')
    
    counts = np.arange(30000, 330000, 30000)
    ref = []
    for col in rowcount.columns:
        ax = plt.plot(counts, rowcount[col])
        ref.append(ax[0])
    
    plt.legend(ref, algorithm_names)
    
    plt.ylim(ymin=0)
    export('rowcount_all')


def plot_rowcount():
    global rowcount
    
    setup_axes()
    plt.title('Rowcount Experiment')
    plt.xlabel('Rowcount')
    plt.ylabel('Runtime (s)')
    
    counts = np.arange(30000, 330000, 30000)
    cols = ['spider', 'sindd']
    ref = []
    for col in cols:
        ax = plt.plot(counts, rowcount[col])
        ref.append(ax[0])
    
    plt.legend(ref, ['SPIDER', 'S-INDD'])
    
    plt.ylim(ymin=0)
    export('rowcount')


def plot_columncount():
    global columncount
    
    setup_axes()
    plt.title('Columncount Experiment')
    plt.xlabel('Columncount')
    plt.ylabel('Runtime (s)')
    
    counts = np.arange(18, 108, 18)
    ref = []
    for col in columncount.columns:
        ax = plt.plot(counts, columncount[col])
        ref.append(ax[0])
    
    plt.legend(ref, algorithm_names)
    
    plt.ylim(ymin=0)
    export('columncount')



def plot_distinct():
    global distinct
    
    setup_axes(use_seconds=False)
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
        

# hackedihack
def plot_runtime_shares():
    raw_data = {'spider': [28633, 5090],
                'sindd': [69070, 11544],
                'oldspider': [71683, 3643]}
    
    data = pd.DataFrame(raw_data).reindex(columns=['spider', 'sindd', 'oldspider']).transpose()
    
    setup_axes()
    plt.title('Runtime Contingents on BIOSQL')
    plt.xlabel('Algorithm')
    plt.ylabel('Runtime (ms)')
    
    x = np.arange(len(data))
    b0 = plt.bar(x, data[0])
    b1 = plt.bar(x, data[1], bottom=data[0])
    
    plt.xticks(x, ['Spider', 'S-INDD', 'OldSpider'])
    plt.legend((b0, b1), ('Attribute Creation', 'Discovering INDs'))
    export('runtime_contingents')


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
    

if __name__ == '__main__':

    for name, dataset in zip(dataset_names, datasets):
        plot_dataset(name, dataset)
    
    for name, algorithm in zip(algorithm_names, algorithms):
        plot_algorithm(name, algorithm)
    
    plot_combined_per_dataset()
    plot_combined_per_algorithm()
    
    plot_sindd_partitions()
    plot_rowcount_all()
    plot_rowcount()
    plot_columncount()
    plot_distinct()
    
    #plot_runtime_shares()
    #plot_query_runtime()
    