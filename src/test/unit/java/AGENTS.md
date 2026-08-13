# Unit Test Guidelines

Full
doc: [docs/internal/development/style/unit-testing.md](../../../../docs/internal/development/style/unit-testing.md)

## Naming

- Test class: `{ClassName}Test` -- mirrors the class under test, same package
- Test method: `methodName_scenario_expectedBehavior` (underscores between camelCase segments)
- No `test` prefix on methods -- redundant with `@Test`
- Parameterized tests **must** set an explicit `name` parameter: `@Parameters(name = "...")`

## Structure

Follow Arrange-Act-Assert, separated by blank lines:

```java

@Test
public void resolve_unknownField_returnsEmpty() {
  var resolver = new FieldResolver(defaultIndexDef());

  var result = resolver.resolve(FieldPath.parse("unknown"));

  assertThat(result).isEmpty();
}
```

## Assertions

Prefer Truth (`assertThat(...)`) over `assertTrue`/`assertFalse` -- Truth produces
descriptive failure messages without extra effort. `Assert.equals` and Truth's `isEqualTo` are
equally acceptable.

## Static Imports

Well-known and unambiguous methods _may_ be statically imported in tests (e.g. `assertEquals`,
`assertThat`, `mock`). When modifying existing test files, match whatever convention is used within
the file.

## Test doubles

Prefer: **real object** > **fake** > **mock**

- Use a mock only when a real object or fake would violate isolation, determinism, or speed.
- Never mock solely to avoid constructing an object -- use a test data builder instead.
- Fakes live in `//src/main/java/com/xgen/testing`, marked `testOnly=True`, with their own tests.
- Reusable test helpers should be defined in `//src/main/java/com/xgen/testing` if they are intended
  to be used in more than one test package.

## Tests should be DAMP

Opposed to production code, tests should optimize for readability over code re-use. Each test case
should be fairly self-contained. For instantiating complex objects, follow the Test Data Builder
pattern (https://testing.googleblog.com/2018/02/testing-on-toilet-cleanly-create-test.html).

## No logic in tests

Avoid `if`, `for`, `switch` in test bodies. Use `@DataPoints` + `@Theory` for parameterized cases.

## Bazel

Use the `mongot_java_unit_test_suite` rule in `BUILD` files, not bare `java_test`.
