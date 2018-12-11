# Spark BlockManager


## 描述

## ShuffleRDD读取数据
- ShuffleRDD读到的数据是上一个RDD输出数据，即ShuffleMapTask的输出结果(一个分区数据文件和一个分区对应的偏移量文件)
- 分区数据文件是有序的，先按分区数从小到大排序，再按(key,value)中的key从小到大进行排序
- 通过BlockStoreShuffleReader.read()对Block数据进行读取
- 通过 MapOutputTracker得到[(BlockManagerId,[(ShuffleBlockId,长度)])],相当于有多少台机器进行executor运算，就有多少个BlockManagerId(即有多少个BlockManager进行管理),每个BlockManager管理着多个数据分区(即多个ShuffleBlockId)
- 通过 ShuffleBlockFetcherIterator()进行本地Block和远程Block数据进行处理，会得到一个 (BlockID, InputStream)这样元素的迭代器，然后进行flatMap()操作，返序列化流对象并转化为KeyValueIterator迭代器，通过两个迭代器就可以遍历当前分区的所有输入数据，由于Block数据块默认的大小是128m，也就是每个BlockID最大数据大小是128m，而且是一次只处理一个BlockID数据，所以此时是不会内存益处的
- 然后对迭代器中的(key,value)元素进行遍历，并插入ExternalAppendOnlyMap对象，如果数据满足插入临时文件的条件，会把内存中的数据插入到临时文件，并保存DiskMapIterator对象到变量spilledMaps中，就是防止数据太大，内存溢出

## 本地Block
- 本地Block是BlockManager负责拉取
- 即ShuffleMapTask和ResultTask在同一台机器上运行时，这个时候只需要通过

## 远程Block
- 远和Block是ShuffleClient负责调用，BlockTransferService负责拉取


### 
- ResultTask读取ShuffleMapTask输出的数据，ResultTask读取的RDD分区文件分两种，一种中本台机器上的数据文件(也就是ResultTask和ShuffleMapTask在同一台机器上),这时能过BlockManager就可以读到对应分区的数据文件