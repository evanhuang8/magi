.PHONY: clean setup

clean:
	$(RM) benchmark/*.test benchmark/*.out test/disque/nodes-*.conf

setup:
	./test/setup.sh