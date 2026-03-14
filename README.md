DataFusion Regexp Extract UDF

An implementation of a regexp_extract User-Defined Function (UDF) for Apache DataFusion, designed to provide Spark-like regex extraction behavior for query processing.
Key Features

    Regex Compilation Caching: Utilizes a Moka cache for compiled Regex objects, preventing redundant re-compilation of patterns across row iterations.

    Standardized Semantics: Designed to handle capture groups, negative indices, and out-of-bounds requests by mapping them to NULL or empty strings, consistent with common SQL engine expectations.

    Defensive Design: Includes robust input sanitization to ensure stability and prevent runtime panics when processing malformed patterns or null inputs.

    Extensive Testing: Includes unit and integration tests covering standard use cases, batch processing, and edge-case scenarios.

Project Architecture

    regexp_kernel.rs: The computational core. Handles regex execution and manages the pattern compilation cache.

    regexp_extract.rs: Implements the ScalarUDFImpl trait, providing the necessary interface for the DataFusion query engine.

    utils.rs: Acts as the mediation layer between the UDF interface and the computation kernel. It provides utility functions for safe data extraction and type handling from DataFusion’s ColumnarValue (scalars and arrays).

    main.rs: Serves as the integration entry point, providing examples for both SQL and DataFrame API usage.

Engineering Notes

The implementation focuses on reducing CPU-intensive operations during query execution. By caching compiled Regex objects, we mitigate the performance penalty of frequent pattern instantiation. The addition of utils.rs streamlines the flow between the UDF interface and the core kernel, ensuring that different input types (scalars vs. arrays) are handled consistently - a critical aspect for maintaining efficiency within the DataFusion execution engine.