.PHONY: build run zip

MOUNT_DIR ?= ~/fuse

build:
	./gradlew installDist

zip:
	./gradlew distZip

run:
	./app/build/install/app/bin/app $(MOUNT_DIR)
