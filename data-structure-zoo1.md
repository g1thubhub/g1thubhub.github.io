---
layout: default
---

# Phil's Data Structure Zoo

Solving a problem programatically often involves grouping data items together so they can be conveniently operated on or copied as a single unit -- the items are collected in a **data structure**. Many different data structures have been designed over the past decades, some store **individual** items like phone numbers, others store more complex objects like name/phone number **pairs**. Each has strengths and weaknesses and is more or less suitable for a specific use case. In this article, I will describe and attempt to classify some of the most popular data structures and their actual implementations on three different abstaction levels starting from a Platonic ideal and ending with actual code that is benchmarked:  


- Theoretical level: Data structures/collection types are described irrespective of any concrete implementation and the asymptotic behaviour of their core operations are listed.
- Implementation level: It will be shown how the container classes of a specific programming language relate to the data structures introduced at the previous level -- e.g., despite their name similarity, [Java's _Vector_](https://docs.oracle.com/javase/8/docs/api/java/util/Vector.html) is different from [Scala's](https://www.scala-lang.org/api/current/scala/collection/immutable/Vector.html) or [Clojure's](http://clojuredocs.org/clojure.core/vector) _Vector_ implementation. In addition, asymptotic complexities of core operations will be provided per implementing class.
- Empirical level: Two aspects of the efficiency of data structures will be measured: The memory footprints of the container classes will be determined under different configurations. The runtime performance of operations will be measured which will show to that extend asymptotic advantages manifest themselves in concrete scenarios and what the relative performances of asymptotically equal structures are.

* * * 

## Theoretical Level
Before providing actual speed and space measurement results in the third section, execution time and space can be described in an abstract way as a function of the number of items that a data structure might store. This is traditionally done via [Big O notation](https://en.wikipedia.org/wiki/Big_O_notation) and the following abbrevations are used throughout the tables:
- **C** is constant time, O(1)
- **aC** is [amortized](https://en.wikipedia.org/wiki/Amortized_analysis) constant time
- **eC** is effective constant time
- **Log** is logarithmic time, O(log n)
- **L** is linear time, O(n)

The green, yellow or red background colours in the table cells will indicate how "good" the time complexity of a particular data structure/operation combination is relative to the other combinations. 
<br/><br/>
Table 1
![Table 1](images/1.png) 


The first five entries of Table 1 are **linear** data structures: They have a linear ordering and can only be traversed in one way. By contrast, **Trees** can be traversed in different ways, they consist of hierarchically linked data items that each have a single parent except for the root item. Trees can also be classified as connected **graphs** without cycles; a data item (= _node_ or _vertex_) can be connected to more than two other items in a graph. 
<br/><br/>
Data structures provide many operations for manipulating their elements. The most important ones are the following four core operations which are included above and studied throughout this article:
- **Access**: Read an element located at a certain position 
- **Search**: Search for a certain element in the whole structure
- **Insertion**: Add an element to the structure
- **Deletion**: Remove a certain element
<br/><br/>

Table 1 includes two **probabilistic** data structures, _Bloom Filter_ and _Skip List_.
