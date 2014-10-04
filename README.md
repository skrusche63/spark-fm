![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

## Reactive Factorization Engine

### Context-Aware Recommendations

Recommender systems are an important feature of modern websites. Especially commercial sites benefit from a boost in customer loyalty, click-through rates and revenues when using recommender systems.

The majority of existing approaches to recommender systems focus on recommending the most relevant items to individual users and do not take into account any contextual information, such as time, place and the company of other people.

For personalized recommendations, however, it is not sufficient to consider only *users* and their engagement on a set of *items*. It is important to incorporate contextual information in order to recommend items to users *under certain circumstances*, and make those recommendations more personalized.

> For example, using a temporal context, a travel recommender system would provide vacation recommendations in the winter, that can be very different from those in the summer.

### Factorization Machines

Matrix factorization (MF) approaches to recommendations have become very popular as they usually outperform traditional k-nearest neighbor methods. MF, however, is context-unaware.

The most approaches to take contextual information into account focus on contextual pre- or post-filtering where a standard context-unaware technique, such as MF is applied to the filter data.

New approaches extend the 2-dimensional space (user,item,rating) `R: U x I -> R` to an n-dimensional space (user,item,c1,c2,...,rating) `R: U x I x C1 x C2 x ... -> R`, where *c1*, *c2* etc describe contextual dimensions. These approaches extend matrix models (MF) to tensor models (TF).

Recently a recommendation approach, called *Multiverse Recommendation*, as been proposed that uses Tucker decomposition to factorize these n-dimensional tensor models. However, a drawback of this approach is its computational complexity.

Therefore, we suggest to build a context-aware recommender system on Factorization Machines (FM), recently introduced by Steffen Rendle. This approach results in fast context-aware recommendations as the model equation of FMs can be computed in linear time both in the number of contextual parameters and the selected factorization size.


### Factorization Machines in Spark

This is an implementation of Factorization Machines based on Scala and Apache Spark. It actually uses stochastic gradient descent (SGD) as a learning method while training the model parameters. 

From http://libfm.org: 

> Factorization machines (FM) are a generic approach that allows to mimic most factorization models by feature engineering. This way, factorization machines combine the generality of feature engineering with the superiority of factorization models in estimating interactions between categorical variables of large domain.


For more details, please refer to:
Steffen Rendle (2012): Factorization Machines with libFM, in ACM Trans. Intell. Syst. Technol., 3(3), May.
