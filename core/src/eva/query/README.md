# Query Engine

Our current query engine implementation is a fairly canonical Datalog inference algorithm (QSQR), excepting two 
additions: negation and the embedding of arbitrary code. We have a handful of features which are tightly-coupled 
to the EAV data model and a handful of APIs that Eva provides on top, but under the hood the engine itself is generic 
and works off of declarative query specifications. There is an interface with a single function that the database 
implements to allow the query engine to use the database as an extensional data source efficiently. 

## References

- Abiteboul, S., Hull, R., & Vianu, V. (1995). Foundations of databases: the logical level. Addison-Wesley Longman Publishing Co., Inc.
  - [PDF](http://webdam.inria.fr/Alice/)
  - See Chapters 13–15

- Ullman, J. D. (1989). Principles of database and knowledge-base systems: Volume II: The new technologies. Rockville: Computer Science Press.
  - See Chapters 12–13
  - See Chapter 12 for top-down search and QSQR
  
- Ceri, S., Gottlob, G., & Tanca, L. (1989). What you always wanted to know about Datalog (and never dared to ask). IEEE transactions on knowledge and data engineering, 1(1), 146-166.
  - [PDF](https://www.utdallas.edu/~gupta/courses/acl/papers/datalog-paper.pdf)
  
- Kim, W., Nicolas, J. M., & Nishio, S. (2014). Deductive and Object-Oriented Databases: Proceedings of the First International Conference on Deductive and Object-Oriented Databases (DOOD89) Kyoto Research Park, Kyoto, Japan, 4-6 December 1989. Elsevier.
  - [Google Books](https://books.google.com/books?hl=en&lr=&id=HbajBQAAQBAJ&oi=fnd&pg=PP1&dq=Proceedings+of+the+First+International+Conference+on+Deductive+and+Object-Oriented+Databases&ots=RTqRR58jMa&sig=dmxsRE2ZaIyc6zkRaPKv0thRQHc#v=onepage&q=Proceedings%20of%20the%20First%20International%20Conference%20on%20Deductive%20and%20Object-Oriented%20Databases&f=false)
  - [PDF](https://core.ac.uk/download/pdf/12169274.pdf)
   
- Ramakrishnan, R., & Ullman, J. D. (1995). A survey of deductive database systems. The journal of logic programming, 23(2), 125-149.
  - [PDF](https://www.sciencedirect.com/science/article/pii/0743106694000399) 

- Bancilhon, F., & Ramakrishnan, R. (1989). An amateur's introduction to recursive query processing strategies. In Readings in Artificial Intelligence and Databases (pp. 376-430). Morgan Kaufmann.
  - [PDF](https://minds.wisconsin.edu/bitstream/handle/1793/58980/TR772.pdf?sequence=1)

- Madalińska-Bugaj, E., & Nguyen, L. A. (2012). A generalized QSQR evaluation method for Horn knowledge bases. ACM Transactions on Computational Logic (TOCL), 13(4), 32.
  - [PDF](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.184.3062&rep=rep1&type=pdf)

- Henschen, L. J. Synthesizing Least Fixed Point Queries into Non-recursive Iterative Programs Shamim A. Naqvi Bell Laboratories Murray Hill, NJ 07974.
  - [PDF](https://www.researchgate.net/profile/Lawrence_Henschen/publication/220812483_Synthesizing_Least_Fixed_Point_Queries_Into_Non-Recursive_Iterative_Programs/links/00b7d524b715fc035a000000.pdf)

- Cao, S. T. (2015). Query-subquery nets with stratified negation. In Advanced Computational Methods for Knowledge Engineering (pp. 355-366). Springer, Cham.
  - [PDF](https://www.researchgate.net/profile/Son_Cao/publication/276349954_Query-Subquery_Nets_with_Stratified_Negation/links/55ad616b08aee079921e2425/Query-Subquery-Nets-with-Stratified-Negation.pdf)

- Bidoit, N. (1991). Negation in rule-based database languages: a survey. Theoretical computer science, 78(1), 3-83.
  - [PDF](https://www.sciencedirect.com/science/article/pii/0304397551900035)

- Henglein, F., & Larsen, K. F. (2010). Generic multiset programming with discrimination-based joins and symbolic Cartesian products. Higher-Order and Symbolic Computation, 23(3), 337-370.
  - [PDF](https://www.cs.ox.ac.uk/projects/utgp/school/henglein2010d.pdf)
