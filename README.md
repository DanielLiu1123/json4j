# json4j

Minimal, standard-first JSON writer and parser. One single Java file, 1k LoC, no dependencies.

## Quick start

Use as dependency:
```groovy
implementation 'com.github.danielliu1123:json4j:+'
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
