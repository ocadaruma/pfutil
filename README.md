# pfutil

[![Build Status](https://travis-ci.org/ocadaruma/pfutil.svg?branch=master)](https://travis-ci.org/ocadaruma/pfutil)
[![Javadocs](https://www.javadoc.io/badge/com.mayreh/pfutil.svg)](https://www.javadoc.io/doc/com.mayreh/pfutil)

A Java HyperLogLog implementation which is compatible with Redis

## Installation

Maven:

```xml
<dependency>
  <groupId>com.mayreh</groupId>
  <artifactId>pfutil</artifactId>
  <version>0.0.2</version>
</dependency>
```

Gradle:

```groovy
dependencies {
    compile 'com.mayreh:pfutil:0.0.2'
}
```

## Usage

```java
import com.mayreh.pfutil.v4.HllV4;
import java.nio.file.*;

HllV4 hll = HllV4.newBuilder().build();

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

See [example project](https://github.com/ocadaruma/pfutil/tree/master/example) for more usage.
