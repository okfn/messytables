run:    build
	@docker run \
	    --rm \
		-ti \
	    messytables

build:
	@docker build -t messytables .

test:
	nosetests --with-coverage --cover-package=messytables --cover-erase

.PHONY: run build test
