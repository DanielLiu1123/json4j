# json4j [![Build](https://img.shields.io/github/actions/workflow/status/DanielLiu1123/json4j/build.yml?branch=main)](https://github.com/DanielLiu1123/json4j/actions) [![Maven Central](https://img.shields.io/maven-central/v/io.github.danielliu1123/json4j)](https://central.sonatype.com/artifact/io.github.danielliu1123/json4j) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Minimal, standard-first JSON writer and parser. One single Java file, 1k LoC, no dependencies.

> [!NOTE]\
> **Performance is not the goal of this library.** 
> If you need a high-performance JSON processing library, consider using Jackson, Gson, or other mature solutions.

## Quick start

Use as dependency:
```groovy
implementation("io.github.danielliu1123:json4j:+")
```

Use as source file:
```shell
mkdir -p json && curl -L -o json/Json.java https://raw.githubusercontent.com/DanielLiu1123/json4j/refs/heads/main/json4j/src/main/java/json/Json.java
```

There are only two APIs:

```java
record Point(int x, int y) {}

// 1) Write JSON
Point point = new Point(1, 2);
String json = Json.stringify(point);
// {"x":1,"y":2}

// 2) Read JSON

// 2.1) Simple type
String json = "{\"x\":1,\"y\":2}";
Point point = Json.parse(jsonString, Point.class);
// Point{x=1, y=2}

// 2.2) Generic type
String json = "[{\"x\":1,\"y\":2},{\"x\":3,\"y\":4}]";
List<Point> points = Json.parse(jsonString, new Json.Type<List<Point>>() {});
// [Point{x=1, y=2}, Point{x=3, y=4}]
```

## License

The MIT License.
