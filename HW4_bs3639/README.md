# Homework 4
## Assignment 1: Review your classmate's Citibike project proposal.
Peer review of Anupama Santhosh's [Cikibike assignment](https://github.com/bensteers/PUI2017_as11566/tree/master/HW3_as11566).


## Assignment 2: Literature choices of statistical tests.
Analyse 3 statistical tests from articles found in [PLOS ONE](http://journals.plos.org/plosone/). List their variables and types along with the hypothesis, research question, and statistical significance level.

| **Statistical Analyses	|  IV(s)  |  IV type(s) |  DV(s)  |  DV type(s)  |  Control Var | Control Var type  | Question to be answered | _H0_ | alpha | link to paper **| 
|:----------:|:----------|:------------|:-------------|:-------------|:------------|:------------- |:------------------|:----:|:-------:|:-------|
| ANOVA |adult performance level (professional, semi-professional, non-professional)|1, categorical|sprint, agility, dribbling, ball control, shooting|5, continuous|age|categorical|Do motor skills during early adolescence offer predictive value for success in soccer later in life|to-be-professional motor skills = to-be semi-professional motor skills = to-be non-professional motor skills|0.001| [The influence of speed abilities and technical skills in early adolescence on adult success in soccer: A long-term prospective analysis using ANOVA and SEM approaches](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0182211) |
|Path analysis|perceived emotion|1, categorical|acoustic measures, proximal percepts|8, continuous|||Which computation paths are responsible for which perceived emotions|path coefficient = 0|0.02|[Path Models of Vocal Emotion Communication](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0136675)|
Logistic Regression |AUC|1, continuous|various classifiers|5, categorical|||Does their predictor predict cases of Leukemia better than existing classifiers|AUC of AML positive = AUC of healthy|0.05| [Leukemia Prediction Using Sparse Logistic Regression](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0072932)|



## Assignment 3: Reproduce the analysis of the Hard to Employ program in NY.
Reproduce the analysis of the Hard to Employ programs for NY. You may want to read the relevant portion of the study to make sure you are not just workign mechanically (Chapter 2 of the original document).

Reproduce the results in cell 2 of Table 2.1 (Ever employed in a CEO transitional job), and cell 10 (Convicted of a felony). Fill in the cells of the scheleton notebook as you are asked to.

Turn in your version of the python notebook in the HW4_<netID> directory

Grading

All cells that are marked "for you to do" (or similar...) and that contain missing values should be filled.

The second null hypothesis should be stated (for the "Convicted of a felony after 3 years" data).

Both tests, Z and chi-sq, should be completed for the "Convicted of a felony after 3 years" data.

The result of the test in term the rejection of the Null should be stated in all cases (for both tests and both for the original "Ever employed in a CEO transitional job" data and the "Convicted of a felony after 3 years data").


## Assignment 4: Tests of correlation using the scipy package with citibike data.
Use the following are 3 tests to assess correlation between 2 samples of citibike data:

Pearson’s test
Spearman’s test
K-S test
There is a skeleton notebook that works on a similar question, age of male vs female riders. Follow it to see how to set up the assignment notebook citibikes_compare_distributions.ipynb.

Use: trip duration of bikers that ride during the day vs night. State your result in words in terms of the Null Hypothesis

Use: age of bikers for trips originating in Manhattan and in Brooklyn. Use at least 2 months of citibike data. The citibike data can be accessed from the citibike website - make sure you do it in a reproducible way, or in the CUSP data facility at the path /gws/open/Student/citibike

Grading

A notebook should be completed as the cell by cell instructions indicate.

You must state the Null Hypothesis, according to what you know about the test and the scipy.stats package documentation for three scipy.stats function, corresponding to the three tests.

You must put the caluclated statistics and the p-value in the context of null hypothesis rejection in each case.
