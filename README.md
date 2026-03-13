DataFusion Regexp Extract UDF

This project implements a high-performance custom regexp_extract User-Defined Function (UDF) for Apache DataFusion. It is designed to provide robust regex extraction while ensuring system stability and high throughput through optimized caching mechanisms.

Key Features

    High Performance: Utilizes Moka Cache to manage both regex compilation and extraction results, significantly reducing CPU overhead for repeated patterns and inputs.

    Memory Optimization: Implements FxHash (via rustc-hash) to represent keys as u64 instead of strings, effectively preventing heap fragmentation and reducing the overall memory footprint in high-concurrency environments.

    Defensive Programming: Provides comprehensive protection against Panic scenarios, such as negative indices, by enforcing strict input validation and mapping invalid states to standard SQL NULL or empty strings.

    Comprehensive Testing: Includes unit tests, integration tests, and performance benchmarks to ensure correctness and efficiency under load.

Project Architecture

    regexp_kernel.rs: The computational core. Handles regex execution, cache management, and input sanitization.

    regexp_extract.rs: Implements the ScalarUDFImpl trait, providing the necessary interface for DataFusion’s query engine.

    main.rs: Contains end-to-end integration tests and usage examples for both SQL and DataFrame APIs.

Engineering Notes

In high-throughput systems, using FxHash for cache keys allows us to bypass the overhead of hashing and storing long strings, leading to significant performance gains-demonstrating a substantial speed improvement in regex compilation scenarios compared to raw execution.