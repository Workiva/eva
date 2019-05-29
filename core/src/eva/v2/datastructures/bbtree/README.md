# B<sup>ε</sup>-Trees

This package contains implementations of our [B<sup>ε</sup>-tree](http://cs.au.dk/~gerth/papers/alcomft-tr-03-75.pdf), which we use for Eva's indexes. It sits somewhere along the Pareto front of datastructure tradeoffs, and optimizes to be write-limited. B<sup>ε</sup> trees are sometimes called "buffer trees," for their main distinguishing feature: rather than propagating updates immediately down the tree to the leaves, each operation on the root of the tree adds itself to a message buffer sitting on that root; once that message buffer has exceeded a certain size, messages are selected to be pushed down to the next layer in the tree, where again they are lazily inserted into message buffers. The only nodes in the tree that do not contain message buffers are the leaves themselves: once a message reaches a leaf, the operation encoded by that message is simply applied (insert/delete/etc.).

This design has several notable ramifications for implemention:

1. The code responsible for performing reads of the tree must know how to push messages down the tree and how to apply them at the leaves; the laziness of the writer is paid for by the readers. The extent of this tradeoff can be tuned by modifying the value of *ε*.
2. Range operations on B<sup>+</sup>-trees traditionally work by finding the leaf corresponding to the lower bound of the affected range, then traversing a linked list along the leaves to apply operations. However, in a B<sup>ε</sup>-tree, this would effectively "skip" an untold number of relevant messages present in the tree, while a naive tree traversal would be ridiculously inefficient. B<sup>ε</sup> implementations instead use<sup>[1](http://brics.dk/RS/96/28/BRICS-RS-96-28.pdf) [2](https://www.cc.gatech.edu/~bader/COURSES/GATECH/CSE-Algs-Fall2013/papers/Arg03.pdf)</sup> some variation of batched patterns for range operations.

## Version 0

This version makes heavy use of [persistence](https://www.cs.cmu.edu/~rwh/theses/okasaki.pdf) and [laziness](https://www.cc.gatech.edu/~bader/COURSES/GATECH/CSE-Algs-Fall2013/papers/Arg03.pdf) and [batching](http://brics.dk/RS/96/28/BRICS-RS-96-28.pdf). Our deletion algorithm is derived from [this paper](http://ilpubs.stanford.edu:8090/85/1/1995-19.pdf). See [here](./logic/v0/README.md) for implementation-specific details.

## References
- [Write-Optimization in B-Trees](http://www.hpts.ws/papers/2013/hpts13-panel.pdf)
- [GUIDs as fast primary keys under multiple databases](https://www.codeproject.com/Articles/388157/GUIDs-as-fast-primary-leys-%20-under-multiple-database)
- [The Buffer Tree: A New Technique for Optimal I/O Algorithms](https://pdfs.semanticscholar.org/5353/ed87f7ec15c32d66f81a0ad9ba2f695f0855.pdf)
- [The Buffer Tree: A Technique for Designing Batched External Data Structures](https://www.cc.gatech.edu/~bader/COURSES/GATECH/CSE-Algs-Fall2013/papers/Arg03.pdf)
- [Streaming Cache-Oblivious B-Trees](http://supertech.csail.mit.edu/papers/sbtree.pdf)
- [Lower Bounds for External Memory Dictionaries](http://www.cs.au.dk/~gerth/papers/alcomft-tr-03-75.pdf)
- [Persistent Sorted Maps and Sets with Log-Time Rank Queries](https://github.com/clojure/data.avl)
- [Implementing Deletion in B+-Trees](http://ilpubs.stanford.edu/85/1/1995-19.pdf)
- [Purely Functional Data Structures](https://www.cs.cmu.edu/~rwh/theses/okasaki.pdf)

## Resources for Future Work
http://publications.csail.mit.edu/abstracts/abstracts07/jfineman/jfineman.html
