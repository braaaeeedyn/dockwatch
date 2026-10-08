# Forwards to tasks.py so `make test` (CI, Linux) and `python tasks.py test` (Windows) run the same thing.
%:
	python tasks.py $@ $(ARGS)
