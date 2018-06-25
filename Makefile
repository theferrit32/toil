# Copyright (C) 2015-2018 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

define help

Supported targets: prepare, develop, docs, sdist, clean, test, pypi, docker and push_docker.

Please note that all build targets require a virtualenv to be active.

The 'prepare' target installs Toil's build requirements into the current virtualenv.

The 'develop' target creates an editable install of Toil and its runtime requirements in the
current virtualenv. The install is called 'editable' because changes to the source code
immediately affect the virtualenv. Set the 'extras' variable to ensure that the 'develop' target
installs support for extras. Consult setup.py for the list of supported extras. To install Toil
in develop mode with all extras, run

	make develop extras=[all]

The 'sdist' target creates a source distribution of Toil. It is used for some unit tests and for
installing the currently checked out version of Toil into the appliance image.

The 'clean' target cleans up the side effects of 'develop', 'sdist', 'docs', 'pypi' and 'docker'
on this machine. It does not undo externally visible effects like removing packages already
uploaded to PyPI.

The 'docs' target uses Sphinx to create HTML documentation in the docs/_build directory

The 'test' target runs Toil's unit tests serially with pytest. It will run some docker tests and
setup. If you wish to avoid this, use the 'test_offline' target instead. Note: this target does not
capture output from the terminal. For any of the test targets, set the 'tests' variable to run a
particular test, e.g.

	make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The 'test_offline' target is similar to 'test' but it skips the docker dependent tests and their
setup.

The 'integration_test_local' target runs toil's integration tests. These are more thorough but also
more costly than the regular unit tests. For the AWS integration tests to run, the environment
variable 'TOIL_AWS_KEYNAME' must be set. This user will be charged for expenses acrued during the
test. This test does not capture terminal output.

The 'integration_test' target is the same as the previous except that it does capture output.

The 'test_parallel' target runs Toil's unit tests in parallel and generates an XML test report
from the results. It is designed to be used only in Jenkins.

The 'pypi' target publishes the current commit of Toil to PyPI after enforcing that the working
copy and the index are clean.

The 'docker' target builds the Docker images that make up the Toil appliance. You may set the
TOIL_DOCKER_REGISTRY variable to override the default registry that the 'push_docker' target pushes
the appliance images to, for example:

	TOIL_DOCKER_REGISTRY=quay.io/USER make docker

If Docker is not installed, Docker-related targets tasks and tests will be skipped. The
same can be achieved by setting TOIL_DOCKER_REGISTRY to an empty string.

The 'push_docker' target pushes the Toil appliance images to a remote Docker registry. It
requires the TOIL_DOCKER_REGISTRY variable to be set to a value other than the default to avoid
accidentally pushing to the official Docker registry for Toil.

The TOIL_DOCKER_NAME environment variable can be set to customize the appliance image name that
is created by the 'docker' target and pushed by the 'push_docker' target. The Toil team\'s
continuous integration system overrides this variable to avoid conflicts between concurrently
executing builds for the same revision, e.g. toil-pr and toil-it.

endef
export help
help:
	@printf "$$help"

# This Makefile uses bash features like printf and <()
SHELL=bash
python=python2.7
pip=pip2.7
tests=src
tests_local=src/toil/test
# do slightly less than travis timeout of 10 min.
pytest_args_local=-vv --timeout=530
extras=

dist_version:=$(shell $(python) version_template.py distVersion)
sdist_name:=toil-$(dist_version).tar.gz

docker_tag:=$(shell $(python) version_template.py dockerTag)
default_docker_registry:=$(shell $(python) version_template.py dockerRegistry)
docker_path:=$(strip $(shell which docker))

ifdef docker_registry
    export TOIL_DOCKER_REGISTRY?=$(docker_registry)
else
    export TOIL_DOCKER_REGISTRY?=$(default_docker_registry)
endif
export TOIL_DOCKER_NAME?=$(shell $(python) version_template.py dockerName)
export TOIL_APPLIANCE_SELF:=$(TOIL_DOCKER_REGISTRY)/$(TOIL_DOCKER_NAME):$(docker_tag)

ifndef BUILD_NUMBER
green=\033[0;32m
normal=\033[0m
red=\033[0;31m
cyan=\033[0;36m
endif

develop: check_venv
	$(pip) install -e .$(extras)
clean_develop: check_venv
	- $(pip) uninstall -y toil
	- rm -rf src/*.egg-info
	- rm src/toil/version.py

sdist: dist/$(sdist_name)
dist/$(sdist_name): check_venv
	@test -f dist/$(sdist_name) && mv dist/$(sdist_name) dist/$(sdist_name).old || true
	$(python) setup.py sdist
	@test -f dist/$(sdist_name).old \
	    && ( cmp -s <(tar -xOzf dist/$(sdist_name)) <(tar -xOzf dist/$(sdist_name).old) \
	         && mv dist/$(sdist_name).old dist/$(sdist_name) \
	         && printf "$(cyan)No significant changes to sdist, reinstating backup.$(normal)\n" \
	         || rm dist/$(sdist_name).old ) \
	    || true
clean_sdist:
	- rm -rf dist
	- rm src/toil/version.py


# This target will skip building docker and all docker based tests
test_offline: check_venv check_build_reqs
	@printf "$(cyan)All docker related tests will be skipped.$(normal)\n"
	TOIL_SKIP_DOCKER=True \
		$(python) -m pytest $(pytest_args_local) $(tests_local)

# The auto-deployment test needs the docker appliance
test: check_venv check_build_reqs docker
	TOIL_APPLIANCE_SELF=$(docker_registry)/$(docker_base_name):$(docker_tag) \
	    $(python) -m pytest $(pytest_args_local) $(tests)

# For running integration tests locally in series (uses the -s argument for pyTest)
integration_test_local: check_venv check_build_reqs sdist push_docker
	TOIL_TEST_INTEGRATIVE=True \
		$(python) run_tests.py --local integration-test $(tests)

# These two targets are for backwards compatibility but will be removed shortly
# FIXME when they are removed add check_running_on_jenkins to the jenkins targets
test_parallel: jenkins_test_parallel

integration_test: jenkins_test_integration

# This target is designed only for use on Jenkins
jenkins_test_parallel: check_venv check_build_reqs docker
	$(python) run_tests.py test $(tests)

# This target is designed only for use on Jenkins
jenkins_test_integration: check_venv check_build_reqs sdist push_docker
	TOIL_TEST_INTEGRATIVE=True $(python) run_tests.py integration-test $(tests)


pypi: check_venv check_clean_working_copy check_running_on_jenkins
	$(pip) install setuptools --upgrade
	$(python) setup.py egg_info sdist bdist_egg upload
clean_pypi:
	- rm -rf build/


ifdef TOIL_DOCKER_REGISTRY

docker_image:=$(TOIL_DOCKER_REGISTRY)/$(TOIL_DOCKER_NAME)
docker_short_tag:=$(shell $(python) version_template.py dockerShortTag)
docker_minimal_tag:=$(shell $(python) version_template.py dockerMinimalTag)

grafana_image:=$(TOIL_DOCKER_REGISTRY)/toil-grafana
prometheus_image:=$(TOIL_DOCKER_REGISTRY)/toil-prometheus
mtail_image:=$(TOIL_DOCKER_REGISTRY)/toil-mtail

define tag_docker
	@printf "$(cyan)Removing old tag $2. This may fail but that's expected.$(normal)\n"
	-docker rmi $2
	docker tag $1 $2
	@printf "$(green)Tagged appliance image $1 as $2.$(normal)\n"
endef


docker: docker/Dockerfile
	@set -ex \
	; cd docker \
	; docker build --tag=$(docker_image):$(docker_tag) -f Dockerfile .

	@set -ex \
	; cd dashboard/prometheus \
	; docker build --tag=$(prometheus_image):$(docker_tag) -f Dockerfile .

	@set -ex \
	; cd dashboard/grafana \
	; docker build --tag=$(grafana_image):$(docker_tag) -f Dockerfile .

	@set -ex \
	; cd dashboard/mtail \
	; docker build --tag=$(mtail_image):$(docker_tag) -f Dockerfile .

ifdef BUILD_NUMBER
	$(call tag_docker,$(docker_image):$(docker_tag),$(docker_image):$(docker_short_tag))
	$(call tag_docker,$(docker_image):$(docker_tag),$(docker_image):$(docker_minimal_tag))
endif

docker/$(sdist_name): dist/$(sdist_name)
	cp $< $@

docker/Dockerfile: docker/Dockerfile.py docker/$(sdist_name)
	_TOIL_SDIST_NAME=$(sdist_name) $(python) docker/Dockerfile.py > $@

clean_docker:
	-rm docker/Dockerfile docker/$(sdist_name)
	-docker rmi $(docker_image):$(docker_tag)

obliterate_docker: clean_docker
	-@set -x \
	; docker images $(docker_image) \
	    | tail -n +2 | awk '{print $$1 ":" $$2}' | uniq \
	    | xargs docker rmi
	-docker images -qf dangling=true | xargs docker rmi

push_docker: docker check_docker_registry
	for i in $$(seq 1 5); do docker push $(docker_image):$(docker_tag) && break || sleep 60; done
	for i in $$(seq 1 5); do docker push $(grafana_image):$(docker_tag) && break || sleep 60; done
	for i in $$(seq 1 5); do docker push $(prometheus_image):$(docker_tag) && break || sleep 60; done
	for i in $$(seq 1 5); do docker push $(mtail_image):$(docker_tag) && break || sleep 60; done

else

docker docker_push clean_docker:
	@printf "$(cyan)Skipping '$@' target as TOIL_DOCKER_REGISTRY is empty or Docker is not installed.$(normal)\n"

endif


docs: check_venv check_build_reqs
	# Strange, but seemingly benign Sphinx warning floods stderr if not filtered:
	cd docs && make html
clean_docs: check_venv
	- cd docs && make clean


clean: clean_develop clean_sdist clean_pypi clean_docs


check_build_reqs:
	@$(python) -c 'import mock; import pytest' \
		|| ( printf "$(red)Build requirements are missing. Run 'make prepare' to install them.$(normal)\n" ; false )


prepare: check_venv
	$(pip) install sphinx==1.5.5 mock==1.0.1 pytest==2.8.3 stubserver==1.0.1 \
		pytest-timeout==1.2.0 cwltest


check_venv:
	@$(python) -c 'import sys, os; sys.exit( int( 0 if "VIRTUAL_ENV" in os.environ else 1 ) )' \
		|| ( printf "$(red)A virtualenv must be active.$(normal)\n" ; false )


check_clean_working_copy:
	@printf "$(green)Checking if your working copy is clean ...$(normal)\n"
	@git diff --exit-code > /dev/null \
		|| ( printf "$(red)Your working copy looks dirty.$(normal)\n" ; false )
	@git diff --cached --exit-code > /dev/null \
		|| ( printf "$(red)Your index looks dirty.$(normal)\n" ; false )
	@test -z "$$(git ls-files --other --exclude-standard --directory)" \
		|| ( printf "$(red)You have untracked files:$(normal)\n" \
			; git ls-files --other --exclude-standard --directory \
			; false )


check_running_on_jenkins:
	@printf "$(green)Checking if running on Jenkins ...$(normal)\n"
	@test -n "$$BUILD_NUMBER" \
		|| ( printf "$(red)This target should only be invoked on Jenkins.$(normal)\n" ; false )


check_docker_registry:
	@test "$(default_docker_registry)" != "$(TOIL_DOCKER_REGISTRY)" || test -n "$$BUILD_NUMBER" \
		|| ( printf '$(red)Please set TOIL_DOCKER_REGISTRY to a value other than \
	$(default_docker_registry) and ensure that you have permissions to push \
	to that registry. Only CI builds should push to $(default_docker_registry).$(normal)\n' ; false )


check_cpickle:
	# fail if cPickle.dump(s) called without HIGHEST_PROTOCOL
	# https://github.com/BD2KGenomics/toil/issues/1503
	! find src -iname '*.py' | xargs grep 'cPickle.dump' | grep --invert-match HIGHEST_PROTOCOL


.PHONY: help \
		prepare \
		check_cpickle \
		develop clean_develop \
		sdist clean_sdist \
		test test_offline test_parallel integration_test \
		jenkins_test_parallel jenkins_test_integration \
		pypi clean_pypi \
		docs clean_docs \
		clean \
		check_venv \
		check_clean_working_copy \
		check_running_on_jenkins \
		check_build_reqs \
		docker clean_docker push_docker
