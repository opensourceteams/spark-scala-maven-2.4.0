# Hadoop RDD partition划分图解

## HDFS数据文件

```shell
a b k l j
c a n m o
```

## HDFS 数据文件图解

[![](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_1.png)](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_1.png)

## HDFS 数据文件图解(对比）
### 图一
[![](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_2.png)](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_2.png)
### 图二
[![](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_3.png)](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_3.png)

## HadoopRDD partition预划分方式(实际会有小的调整)
- 每个partition的长度= 文件的总长度 / 最小的分区数(默认分区数为2)  //注意，是除，结果会取整, 即 goalSize = totalSize / numSplits
- 示例中每个partition的长度 = 20 / 2 =10  // 即为10个byte
- 然后依次从0开始划分10个byte长度为一个partition,最后一个小于等于10个byte的为最后一个partition
- 所以  parition(0) = hdfs文件(0 + 10) //即从文件偏移量为0开始，共10byte,0 <= 值 < 10
- 所以  parition(1) = hdfs文件(10 + 10) //即从文件偏移量为10开始，共10byte,10 <= 值 < 20
- 即 partition(i) = hdfs文件( i * goalSize + 10 )

## HadoopRDD partition划分原理
- 由于需要考虑，每个partition谁先执行是不确定的，所以每个partition执行时，都需要可明确计算当前partition的数据范围
- 由于直接按partition预划分方式，会把有的一行数据拆分，有些场景不适合
- 所以需要按行做为最小的数据划分单元，来进行partition的数据范围划分
- HadoopRDD是这样划分的partition,还是按partition预划分方式进行预先划分，不过在计算时会进行调整
- 对于首个partition,也就是partition(0),分区数据范围的开始位置就是从0开始(0 + goalSize )
- 对于非首个partition，的开始位置需要从新计算，从预划分的当前partition的开始位置开始找第一个换行符位置(indexNewLine),当前partition的开始位置为= indexNewLine + 1,长度还是goalSize
- 对于首个partition一定能分到数据(只要HDFS文件有数据)
- 非首个partition,有可能分不到数据的情况，分不到数据的情况，就是数据被上一个partition划分完了，而且只对partition数据是不跨行的，如果数据跨行，也是一定会被分到数据的

### partition分不到数据(以下情况同时满足)
- 是非首个partition,也就是不是partition为索引为0
- partition的数据没有跨行，也就是在同一行里
- partition的预分区范围内的第一个换行符\n在该行的结尾


## 源码分析
- HadoopRDD

```scala
 override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)
      val jobConf = getJobConf()

      val inputMetrics = context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Hadoop)

      // Sets the thread local variable for the file's name
      split.inputSplit.value match {
        case fs: FileSplit => SqlNewHadoopRDDState.setInputFileName(fs.getPath.toString)
        case _ => SqlNewHadoopRDDState.unsetInputFileName()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = inputMetrics.bytesReadCallback.orElse {
        split.inputSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
          case _ => None
        }
      }
      inputMetrics.setBytesReadCallback(bytesReadCallback)

      var reader: RecordReader[K, V] = null
      //返回TextInputFormat对象
      val inputFormat = getInputFormat(jobConf)
      HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format(createTime),
        context.stageId, theSplit.index, context.attemptNumber, jobConf)
      //实例化对象 org.apache.hadoop.mapred.LineRecordReader
      //new LineRecordReader()实例方法中， 并且会重新计算当前partition的开始位置(与预分区的会有出入)
      reader = inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener{ context => closeIfNeeded() }
      val key: K = reader.createKey()
      val value: V = reader.createValue()

      override def getNext(): (K, V) = {
        try {
          //调用 org.apache.hadoop.mapred.LineRecordReader.next()方法
          finished = !reader.next(key, value)
        } catch {
          case _: EOFException if ignoreCorruptFiles => finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        //返回当前一对(key,value)对应的值
        (key, value)
      }

      override def close() {
        if (reader != null) {
          SqlNewHadoopRDDState.unsetInputFileName()
          // Close the reader and release it. Note: it's very important that we don't close the
          // reader more than once, since that exposes us to MAPREDUCE-5918 when running against
          // Hadoop 1.x and older Hadoop 2.x releases. That bug can lead to non-deterministic
          // corruption issues when reading compressed input.
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (bytesReadCallback.isDefined) {
            inputMetrics.updateBytesRead()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit] ||
                     split.inputSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.inputSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }
```

- TextInputFormat
- 返回LineRecordReader

```scala
  public RecordReader<LongWritable, Text> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
    throws IOException {
    
    reporter.setStatus(genericSplit.toString());
    String delimiter = job.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }
    return new LineRecordReader(job, (FileSplit) genericSplit,
        recordDelimiterBytes);
  }
```

- LineRecordReader
- 实例方法中，重新定位当前partition的开始位置
- 如果是partition(0),开始位置是0
- 如果不是partition(0),开始位置重新计算
- 调用 in.readLine()方法,等于调用 UncompressedSplitLineReader.readLine(),注意此时传的maxLineLength参数为0

```scala
public LineRecordReader(Configuration job, FileSplit split,
      byte[] recordDelimiter) throws IOException {
    this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
      LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);
    if (isCompressedInput()) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn =
          ((SplittableCompressionCodec)codec).createInputStream(
            fileIn, decompressor, start, end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK);
        in = new CompressedSplitLineReader(cIn, job, recordDelimiter);
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn; // take pos from compressed stream
      } else {
        in = new SplitLineReader(codec.createInputStream(fileIn,
            decompressor), job, recordDelimiter);
        filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
      in = new UncompressedSplitLineReader(
          fileIn, job, recordDelimiter, split.getLength());
      filePosition = fileIn;
    }
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) {
      start += in.readLine(new Text(), 0, maxBytesToConsume(start));
    }
    this.pos = start;
  }
```

- UncompressedSplitLineReader.readLine()
- 调用LineReader.readLine()方法

```scala
@Override
  public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {
    int bytesRead = 0;
    if (!finished) {
      // only allow at most one more record to be read after the stream
      // reports the split ended
      if (totalBytesRead > splitLength) {
        finished = true;
      }

      bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
    }
    return bytesRead;
  }
```

- LineReader.readLine()方法
- 调用  LineReader.readDefaultLine()方法

```scala
/**
   * Read one line from the InputStream into the given Text.
   *
   * @param str the object to store the given line (without newline)
   * @param maxLineLength the maximum number of bytes to store into str;
   *  the rest of the line is silently discarded.
   * @param maxBytesToConsume the maximum number of bytes to consume
   *  in this call.  This is only a hint, because if the line cross
   *  this threshold, we allow it to happen.  It can overshoot
   *  potentially by as much as one buffer length.
   *
   * @return the number of bytes read including the (longest) newline
   * found.
   *
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength,
                      int maxBytesToConsume) throws IOException {
    if (this.recordDelimiterBytes != null) {
      return readCustomLine(str, maxLineLength, maxBytesToConsume);
    } else {
      return readDefaultLine(str, maxLineLength, maxBytesToConsume);
    }
  }

```


- LineReader.readDefaultLine()方法
- 具体计算partition的开始位置的方法
- 注意，此时传过来的maxLineLength参数值为0，也就是先不实际读取数据放到(key,value)的value中

```scala
/**
   * Read a line terminated by one of CR, LF, or CRLF.
   */
  private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume)
  throws IOException {
    /* We're reading data from in, but the head of the stream may be
     * already buffered in buffer, so we have several cases:
     * 1. No newline characters are in the buffer, so we need to copy
     *    everything and read another buffer from the stream.
     * 2. An unambiguously terminated line is in buffer, so we just
     *    copy to str.
     * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
     *    in CR.  In this case we copy everything up to CR to str, but
     *    we also need to see what follows CR: if it's LF, then we
     *    need consume LF as well, so next call to readLine will read
     *    from after that.
     * We use a flag prevCharCR to signal if previous character was CR
     * and, if it happens to be at the end of the buffer, delay
     * consuming it until we have a chance to look at the char that
     * follows.
     */
    str.clear();
    int txtLength = 0; //tracks str.getLength(), as an optimization
    int newlineLength = 0; //length of terminating newline
    boolean prevCharCR = false; //true of prev char was CR
    long bytesConsumed = 0;
    do {
      int startPosn = bufferPosn; //starting from where we left off the last time
      if (bufferPosn >= bufferLength) {
        startPosn = bufferPosn = 0;
        if (prevCharCR) {
          ++bytesConsumed; //account for CR from previous read
        }
        bufferLength = fillBuffer(in, buffer, prevCharCR);
        if (bufferLength <= 0) {
          break; // EOF
        }
      }
      for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
        if (buffer[bufferPosn] == LF) {
          newlineLength = (prevCharCR) ? 2 : 1;
          ++bufferPosn; // at next invocation proceed from following byte
          break;
        }
        if (prevCharCR) { //CR + notLF, we are at notLF
          newlineLength = 1;
          break;
        }
        prevCharCR = (buffer[bufferPosn] == CR);
      }
      int readLength = bufferPosn - startPosn;
      if (prevCharCR && newlineLength == 0) {
        --readLength; //CR at the end of the buffer
      }
      bytesConsumed += readLength;
      int appendLength = readLength - newlineLength;
      if (appendLength > maxLineLength - txtLength) {
        appendLength = maxLineLength - txtLength;
      }
      if (appendLength > 0) {
        str.append(buffer, startPosn, appendLength);
        txtLength += appendLength;
      }
    } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

    if (bytesConsumed > Integer.MAX_VALUE) {
      throw new IOException("Too many bytes before newline: " + bytesConsumed);
    }
    return (int)bytesConsumed;
  }
```

