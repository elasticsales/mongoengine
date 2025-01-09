MongoMallard
============

MongoMallard is a fast ORM-like layer on top of PyMongo, based on MongoEngine.

* Repository: https://github.com/closeio/mongoengine
* See [README_MONGOENGINE](https://github.com/elasticsales/mongoengine/blob/master/README_MONGOENGINE.rst) for MongoEngine's README.
* See [DIFFERENCES](https://github.com/elasticsales/mongoengine/blob/master/DIFFERENCES.md) for differences between MongoEngine and MongoMallard.


Benchmarks
----------

Sample run on a Apple M3 Max running Sonoma 14.6.1

| | MongoEngine | MongoMallard | Speedup |
|---|---|---|---|
| Doc initialization | 10.113us | 3.219us | 3.14x |
| Doc getattr | 0.086us | 0.086us | 1.00x |
| Doc setattr | 0.549us | 0.211us | 2.60x |
| Doc to mongo | 5.991us | 3.181us | 1.88x |
| Load from SON | 12.094us | 0.685us | 17.66x |
| Save to database | 259.094us | 218.945us | 1.18x |
| Load from database | 260.192us | 246.576us | 1.06x |
| Save/delete big object to database | 18.510ms | 8.925ms | 2.07x |
| Serialize big object from database | 4.058ms | 2.346ms | 1.73x |
| Load big object from database | 11.205ms | 0.655ms | 17.11x |

See [tests/benchmark.py](https://github.com/elasticsales/mongoengine/blob/master/tests/benchmark.py) for source code.
