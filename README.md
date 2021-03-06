# Accumulo-Scala
Accumulo-Scala is a light wrapper to provide a fluent api for reading data from Apache Accumulo in Scala.

# Design Goals
* Provide a fluent API
* Avoid needing to close resources explicitly
* Embrace Scala!

## Usage
1. import com.tetra.accumulo_scala.ConnectorOps._
2. val connector = instance.getConnector(user, passwordToken)
3. connector scan ...

## Examples
scan table 'abc' from 'a' to 'z'

    connector scan "abc" from "a" to "z" foreach { e => 
      //do something with each entry ...
    }

scan table 'abc' for multiple ids

    connector scan "abc" in List("a", "b", "c") foreach { e =>
      //do something with each entry ...
    }


scan table 'abc' for only family f and qualifier q

    connector scan "abc" filter "f:q" foreach { e =>
      //do something with each entry ...
    }


## Settings
* com.tetra.accumulo_scala.auths         (DEFAULT: Auths.Empty)
* com.tetra.accumulo_scala.auths.delim   (DEFAULT: ',')
* com.tetra.accumulo_scala.fq.delim      (DEFAULT: ':')
* com.tetra.accumulo_scala.strict        (DEFAULT: false)

## FAQ (sort of ...)
### How many people actually asked any of these questions?
zero

### What if I want to use a BatchScanner?
Then you can ask for a parallel implementation using par.

    scan table 'abc' par 5 foreach { e => ... }


### What if I only want the first 10?  Shouldn't there be a limit like in SQL?
Here is an example of where we embrace scala.

    scan table 'abc' take 10  foreach { e => ... }


### What if I want to skip the first 10? Shouldn't there be a skip like in SQL?
Here, Scala can help ... but eventually we would like to add a server-side iterator for this.

    scan table 'abc' drop 10 foreach { e => ... }

### I have an index table and a data table, how is this going to help me?
Ask and you shall receive!

    connector scan "data" in (connector scan "index" from "a" to "b" map(...)) foreach { e => 
      ... 
    }

### Is there an easy way to see count how many entries are in my results?
Sure is!

    connector.scan("abc").foldLeft(0)((count,_) => count+1)

### Can this be used from Java?
I guess, if you really want to ...

    Connector connector = instance.getConnector("", new PasswordToken());
    ConnectorOps conn = new ConnectorOps(connector);
    CloseableIterator<Entry<Key, Value>> ci = conn.scan("abc").from(new Key("a")).to(new Key("b"));
    try {
      Entry<Key, Value> e;
      while(ci.hasNext()) {
        e = ci.next();
        //do something
      }
    } finally {
      ci.close();
    }

## License
Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html)

## Links
* https://accumulo.apache.org/
* http://www.scala-lang.org/api/current/#package
