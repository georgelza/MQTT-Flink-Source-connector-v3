.DEFAULT_GOAL := help

define HELP

Available commands:

- build: Maven Build the Project/jar's.

- clean: Remove all created target directories

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help


build:
	mvn clean package

clean:
	cd mqtt-job-v3; rm -rf target
	cd mqtt-job-v3; rm dependency-reduced-pom.xml
	cd mqtt-source-v3; rm -rf target
