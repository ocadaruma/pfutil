# pfutil

[![Build Status](https://travis-ci.org/ocadaruma/pfutil.svg?branch=master)](https://travis-ci.org/ocadaruma/pfutil)
[![Javadocs](https://www.javadoc.io/badge/com.mayreh/pfutil.svg)](https://www.javadoc.io/doc/com.mayreh/pfutil)

A Redis compatible HyperLogLog implementation written in Java

## Installation

Maven:

```xml
<dependency>
  <groupId>com.mayreh</groupId>
  <artifactId>pfutil</artifactId>
  <version>0.1.1</version>
</dependency>
```

Gradle:

```groovy
dependencies {
    compile 'com.mayreh:pfutil:0.1.1'
}
```

## Usage

Create HLL data structure.

Redis4 compatible:

```java
import com.mayreh.pfutil.v4.HllV4;

HllV4 hll = HllV4.newBuilder().build();
```

Redis5 compatible:

```java
import com.mayreh.pfutil.v5.HllV5;

HllV5 hll = HllV5.newBuilder().build();
```

Estimate approx distinct count using HLL.

```java
import java.nio.file.*;

hll.pfAdd("elementA".getBytes());
hll.pfAdd("elementB".getBytes());
hll.pfAdd("elementB".getBytes());
hll.pfAdd("elementC".getBytes());

hll.pfCount(); // => 3

// dump to file
Files.write(Paths.get("/path/to/dump"), hll.dumpRepr());
```

You can restore HLL onto Redis from the dump.

```bash
$ redis-cli -x SET foo < /path/to/dump
$ redis-cli PFCOUNT foo
(integer) 3
```

See also [example project](https://github.com/ocadaruma/pfutil/tree/develop/example).

## Performance

- Machine: ThinkPad T470s
  - Intel(R) Core(TM) i7-7600U CPU @ 2.80GHz
  - 24GB RAM
- OS: Ubuntu 18.04.2 LTS
- JDK: openjdk version "11.0.3"

```
Benchmark                Mode  Cnt         Score         Error  Units
HllBenchmark.pfAddV4    thrpt   10  13140894.909 ± 2167628.883  ops/s
HllBenchmark.pfAddV5    thrpt   10  13668326.241 ± 2845088.571  ops/s
HllBenchmark.pfCountV4  thrpt   10      6907.240 ±    3397.389  ops/s
HllBenchmark.pfCountV5  thrpt   10     10575.651 ±    5598.243  ops/s
HllBenchmark.pfMergeV4  thrpt   10      3088.310 ±     589.666  ops/s
HllBenchmark.pfMergeV5  thrpt   10      2854.870 ±     662.526  ops/s
```

See [benchmark](https://github.com/ocadaruma/pfutil/tree/develop/benchmark) for the details.
