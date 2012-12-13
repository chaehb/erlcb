#!/bin/sh
erl -sname erlcb@localhost -pa ebin -pa deps/*/ebin \
	-boot start_sasl \
	-s ex_reloader \
	-s erlcb
