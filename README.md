![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

## Context-Aware Recommendation System (CARS)

The majority of existing approaches to recommender systems focus on recommending the most relevant items to individual users and do not take into account any contextual information, such as time, place and the company of other people.

For personalized recommendations, however, it is not sufficient to consider only *users* and their engagement on a set of *items*. It is important to incorporate contextual information in order to recommend items to users *under certain circumstances*, and make those recommendations more personalized.

> For example, using a temporal context, a travel recommender system would provide vacation recommendations in the winter, that can be very different from those in the summer.

## Factorization Machines in Spark

This is an implementation of Factorization Machines based on Scala and Apache Spark. It actually uses stochastic gradient descent (SGD) as a learning method while training the model parameters. 

From http://libfm.org: 

> Factorization machines (FM) are a generic approach that allows to mimic most factorization models by feature engineering. This way, factorization machines combine the generality of feature engineering with the superiority of factorization models in estimating interactions between categorical variables of large domain.


For more details, please refer to:
Steffen Rendle (2012): Factorization Machines with libFM, in ACM Trans. Intell. Syst. Technol., 3(3), May.
