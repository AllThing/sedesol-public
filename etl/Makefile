WORKERS := 30

.PHONY: all
all:
	@PYTHONPATH='.:..' luigi --module pipeline SedesolPipelines --workers $(WORKERS)

