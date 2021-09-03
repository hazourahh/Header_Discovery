# Relational Header Discovery using Similarity Search in Table Corpus


This repository contains the implementaion of the system presented my publication:

```
Hazar Harmouch, Thorsten Papenbrock, Felix Naumann: Relational Header Discovery using Similarity Search in a Table Corpus. ICDE 2021: 444-455.
```

## Abstract

Column headers are among the most relevant types of meta-data for relational tables, because they provide meaning and context in which the data is to be interpreted. Headers play an important role in many data integration, exploration, and cleaning scenarios, such as schema matching, knowledge base augmentation, and similarity search. Unfortunately, in many cases column headers are missing, because they were never defined properly, are meaningless, or have been lost during data extraction, transmission, or storage. For example, around one third of the tables on the Web have missing headers.

Missing headers leave abundant tabular data shrouded and inaccessible to many data-driven applications. We introduce a fully automated, multi-phase system that discovers table column headers for cases where headers are missing, meaningless, or unrepresentative for the column values. It leverages existing table headers from web tables to suggest human-understandable, representative, and consistent headers for any target table. We evaluate our system on tables extracted from Wikipedia. Overall, 60% of the automatically discovered table headers are exact and complete. Considering more header candidates, top-5 for example, increases this percentage to 72%.

## Building the project
The system is a java maven project. So in order to build the sources, the following development tools/lib are needed:

1. Java JDK 1.8 or later
2. Maven 3.6.3
3. Git
4. Palmetto (https://github.com/dice-group/Palmetto)


## Datasets
1. Webtables: (http://websail-fe.cs.northwestern.edu/TabEL)
2. Schema statistics: (https://web.eecs.umich.edu/~michjc/data/acsdb.html)
