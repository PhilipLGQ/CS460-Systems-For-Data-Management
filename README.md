# CS460 Project: Movie Recommender System

## Author
* Guanqun Liu (guanqun.liu@epfl.ch)

## Introduction
The aim of this project is to expose various programming paradigms that are
featured in modern scale-out data processing frameworks. In this project, we are expected
to implement data processing pipelines over Apache Spark in Scala. For a project overview and
detailed task requirements, please refer to the *.pdf* version of the project description.

## Documentation
### Prerequisites
Please refer to the library versions and dependencies as stated in ```build.sbt``` and files under `project`
folder.

### Folder Structure
```bash
  .
  ├── project
  │   ├── build.properties               
  │   └── plugins.sbt            
  ├── src/main
  │   ├── resources    # Stores log4j properties and testing datasets
  │   ├── scala/app    # Project implementations
  │   │   ├── aggregator/Aggregator.scala    # Task 3: movie rating averager with incremental maintaining for new ratings
  │   │   ├── analytics/SimpleAnalytics.scala    # Task 2: perform simple data manipulation and extract required analytics
  │   │   ├── loaders
  │   │   │   ├── MoviesLoader.scala    # Task 1: loading movies .csv dataset
  │   │   │   ├── RatingsLoader.scala    # Task 1: loading ratings .csv dataset
  │   │   └── recommender
  │   │   │   ├── LSH
  │   │   │   │   ├── LSHIndex.scala    # Task 4.1/4.2: dataset indexing; near-neighbor lookups
  │   │   │   │   ├── MinHash.scala    # Helper hashing class
  │   │   │   │   └── NNLookup.scala    # Task 4.2: near-neighbor lookups
  │   │   │   ├── baseline/BaselinePredictor.scala    # Task 4.3: baseline predictor
  │   │   │   ├── collaborativeFiltering/CollaborativeFiltering.scala    # Task 4.4: collaborative filtering using ALS algorithm
  │   │   │   └── Recommender.scala    # Task 4.5: recommender class for baseline and collaborative filtering predictors
  ├── build.sbt    # Information of libraries with their dependencies/versions
  ├── .gitlab-ci.yml    # Grading pipeline of the project
  ├── .gitignore
```

### Implementation Detail
This implementation keeps the original folder skeleton and only adds new (member) functions
with imports under *scala.app* package. For each class implemented, please refer to the comments
ahead of classes/functions and in-line code descriptions.


