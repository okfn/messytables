run:    build
	@docker run \
	    --rm \
		-ti \
	    messytables

build:
	@docker build -t messytables .

.PHONY: run build
