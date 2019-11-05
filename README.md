# Inclusion Dependency Discovery Evaluation

This repository provides the implementation of several well-know IND discovery algorithms for the [Metanome](www.metanome.de) data profiling framework, as well as the evaluation scripts.

The results of the evaluation are discussed in [**Inclusion Dependency Discovery: An Experimental Evaluation of Thirteen Algorithms**](https://dl.acm.org/citation.cfm?id=3357916).


### Abstract

> Inclusion dependencies are an important type of metadata in relational databases, because they indicate foreign key relationships and serve a variety of data management tasks, such as data linkage, query optimization, and data integration. The discovery of inclusion dependencies is, therefore, a well-studied problem and has been addressed by many algorithms. Each of these discovery algorithms follows its own strategy with certain strengths and weaknesses, which makes it difficult for data scientists to choose the optimal algorithm for a given profiling task.  
> This paper summarizes the different state-of-the-art discovery approaches and discusses their commonalities. For evaluation purposes, we carefully re-implemented the thirteen most popular discovery algorithms and discuss their individual properties. Our extensive evaluation on several real-world and synthetic datasets shows the unbiased performance of the different discovery approaches and, hence, provides a guideline on when and where each approach works best. Comparing the different runtimes and scalability graphs, we identify the best approaches for certain situations and demonstrate where certain algorithms fail.


If you find our work useful, please consider citing it.

```
@inproceedings{dursch2019inclusion,
 author = {D\"{u}rsch, Falco and Stebner, Axel and Windheuser, Fabian and Fischer, Maxi and Friedrich, Tim and Strelow, Nils and Bleifu\ss, Tobias and Harmouch, Hazar and Jiang, Lan and Papenbrock, Thorsten and Naumann, Felix},
 title = {Inclusion Dependency Discovery: An Experimental Evaluation of Thirteen Algorithms},
 booktitle = {Proceedings of the 28th ACM International Conference on Information and Knowledge Management},
 series = {CIKM '19},
 year = {2019},
 pages = {219--228},
 doi = {10.1145/3357384.3357916},
 publisher = {ACM}
}
```
