# Makefile
#
# This Makefile contains build, test, and development targets for MongoDB Search (mongot).
# These targets are available for both community and internal development.
#
# For MongoDB employees: Atlas and Enterprise-specific targets are defined in Makefile.internal.mk
# and should not be added to this file. See Makefile.internal.mk for guidelines on what belongs there.


# https://stackoverflow.com/questions/18136918/how-to-get-current-relative-directory-of-your-makefile
DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

# Use bazelisk when calling bazel.
BAZELISK := $(DIR)/scripts/tools/bazelisk/run.sh
BAZEL = $(BAZELISK)
RED = \033[0;31m
NOCOLOR = \033[0m
COMMUNITY_CONTAINER_VERSION ?= preview

DOCKER_BUILD_OPTS ?=

# default platform
PLATFORM ?= linux_x86_64

PRE_COMMIT := $(shell command -v pre-commit 2> /dev/null)

# general build rules
.PHONY: build
build:
	@$(call BAZEL) build //src/...
	@$(MAKE) tools.precommit.enabled

.PHONY: build.deploy.community
build.deploy.community:
	@echo 'Building community deployable on platform: $(PLATFORM)'
	@$(call BAZEL) build \
		--platforms=//bazel/platforms:$(PLATFORM) \
		//deploy:mongot-community

# linting rules
.PHONY: lint
lint:
	$(call BAZEL) test \
		--build_tests_only \
		--test_tag_filters=lint \
		--test_output=errors \
		//...

.PHONY: sync
sync:
	@$(call BAZEL) sync

.PHONY: check
check: build \
       lint

# tooling rules
.PHONY: tools.bazelisk.install
tools.bazelisk.install:
	@$(DIR)/scripts/tools/bazelisk/install.sh

.PHONY: tools.bazelisk.path
tools.bazelisk.path: tools.bazelisk.install
	@$(DIR)/scripts/tools/bazelisk/path.sh

.PHONY: tools.buildifier.check
tools.buildifier.check:
	@echo 'Running buildifier'
	@$(call BAZEL) run //bazel:buildifier-check || \
	 (echo "${RED}buildifier.check ERROR${NOCOLOR}: Try running 'make tools.buildifier.fix'" && \
	 false)

.PHONY: tools.buildifier.fix
tools.buildifier.fix:
	@$(call BAZEL) run //bazel:buildifier-fix

.PHONY: tools.buf.lint
tools.buf.lint:
	@$(call BAZEL) test --build_tests_only $(shell $(call BAZEL) query 'kind(buf_lint_test, //...)')

.PHONY: tools.fix_unused_deps
tools.fix_unused_deps:
	echo "Note: Running unused_deps currently requires downgrading to bazel 6.5"
	@$(call BAZEL) build @com_github_bazelbuild_buildtools//unused_deps
	@$(shell $(BAZEL) info bazel-bin)/external/com_github_bazelbuild_buildtools/unused_deps/unused_deps_/unused_deps --build_tool=$(BAZEL) //src/... \
		| grep 'deps //src/' `\# ignore 3rd party deps` \
		| tee \
		| bash

.PHONY: tools.checkstyle
tools.checkstyle:
	@echo 'Running checkstyle'
	@$(call BAZEL) test \
	               --build_tests_only \
	               --test_tag_filters='checkstyle' \
	               --test_output=errors \
	               //src/...

.PHONY: tools.gazelle
tools.gazelle:
	@$(call BAZEL) run //bazel/java:gazelle

.PHONY: tools.jdk.fetch
tools.jdk.fetch:
	@$(call BAZEL) fetch @adoptium_jdk_macos_aarch64//...

.PHONY: tools.jdk.path
tools.jdk.path: tools.jdk.fetch
	@echo $(shell $(BAZELISK) info output_base)/external/adoptium_jdk_macos_aarch64/Contents/Home

.PHONY: tools.precommit.enabled
tools.precommit.enabled:
	@if [ -z "$(PRE_COMMIT)" ]; then \
            echo "****************************************************************"; \
    		echo "WARNING: pre-commit is not installed on your system."; \
    		echo "Install it via 'pip install pre-commit' or 'brew install pre-commit'."; \
            echo "****************************************************************"; \
    	elif [ ! -f .git/hooks/pre-commit ] || ! grep -q "pre-commit" .git/hooks/pre-commit; then \
    		echo "****************************************************************"; \
    		echo "WARNING: Local pre-commit hooks are not installed."; \
    		echo "Run 'pre-commit install' to enable it."; \
    		echo "****************************************************************"; \
    	fi

# dependency rules
.PHONY: deps.update
deps.update:
	@$(DIR)/scripts/java/update-dependencies.sh
	@$(call BAZEL) run //bazel/python:requirements.update

.PHONY: deps.outdated
deps.outdated:
	@$(call BAZEL) run @maven//:outdated

.PHONY: test.unit
test.unit:
	@echo 'Running unit tests for files affected by this PR'
	@if python3 $(DIR)/scripts/ci/affected_tests.py \
			--base-branch "$${branch_name:-master}" \
			--bazel "$(BAZEL)" \
			--tag "unit" \
			--output-file "test_filter.targets"; then \
		$(BAZEL) test \
			--test_output=errors \
			--target_pattern_file=test_filter.targets; \
	else \
		echo 'Running all unit tests'; \
		$(BAZEL) test \
			--test_tag_filters='unit' \
			--test_output=errors \
			//src/...; \
	fi

.PHONY: test.unit.coverage
test.unit.coverage:
	@echo 'Running unit tests with coverage'
	@$(call BAZEL) coverage \
	               --instrumentation_filter="-//src/test[/:],+//src/main[/:]" \
	               --combined_report=lcov \
	               --coverage_report_generator=@bazel_tools//tools/test/CoverageOutputGenerator/java/com/google/devtools/coverageoutputgenerator:Main \
	               $(shell $(call BAZEL) query 'attr(tags, unit, tests(//...)) except attr(tags, e2e, tests(//...))')
	@$(DIR)/scripts/tests/combine-coverage-reports.sh
	@open $(shell $(call BAZEL) info output_path)/_coverage/coverage-report/index.html

.PHONY: test.unit.mutate
test.unit.mutate:
	@$(DIR)/scripts/java/run-mutation-test.sh $(CLASS)


.PHONY: test.burn
test.burn: RUNS ?= 1000
test.burn:
	@echo 'Running unit tests'
	@$(call BAZEL) test \
	               --runs_per_test=$(RUNS) \
	               --test_output=errors \
	               $(TARGET)

.PHONY: test.burn.concurrent
test.burn.concurrent: RUNS ?= 1000
test.burn.concurrent: JOBS ?= 250
test.burn.concurrent:
	@echo 'Running unit tests'
	@$(call BAZEL) test \
	               --runs_per_test=$(RUNS) \
	               --jobs=$(JOBS) \
	               --local_test_jobs=$(JOBS) \
	               --test_output=errors \
	               $(TARGET)

.PHONY: test.bisect
test.bisect:
	@echo 'Running git-bisect'
		python3 $(DIR)/scripts/tests/git_bisect_script.py \
    		$(if $(BAZELISK),-z $(BAZELISK)) \
    		$(if $(GOOD),-g $(GOOD)) \
    		$(if $(BAD),-b $(BAD))

.PHONY: tracer.parse
tracer.parse:
	@$(call BAZEL) run //src/main/java/com/xgen/mongot/trace/parser:parser -- $(INPUT) $(DIR)

##############################################################
############# ADD NEW TARGETS ABOVE THIS LINE ################
##############################################################

# Internal targets
-include Makefile.internal.mk
