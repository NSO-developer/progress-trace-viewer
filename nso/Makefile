all: run

run:
	ncs-setup --dest .
	ncs || true
	ncs_load -lm ../progress-trace.xml
	#cli
	ncs_load -lm read-timeout.xml
	ncs --stop
	mv logs/progress-trace.csv progress-trace-`basename ${NCS_DIR}`.csv
	rm -rf README.ncs logs ncs-cdb ncs.conf packages scripts state storedstate target
