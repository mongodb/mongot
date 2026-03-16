load("@aspect_rules_js//npm:repositories.bzl", "npm_translate_lock")

def javascript_deps():
    # Translate the pnpm-lock.yaml for search-index-schema to Bazel targets
    npm_translate_lock(
        name = "npm_search_index_schema",
        pnpm_lock = "//packages/search-index-schema:pnpm-lock.yaml",
        verify_node_modules_ignored = "//:.bazelignore",
        data = [
            "//packages/search-index-schema:package.json",
        ],
    )
