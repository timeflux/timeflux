#/bin/bash


echo "\n* One graph, one node, interrupt with Ctrl+C"
timeflux test_1.yaml

echo "\n* One graph, one node, automatic interrupt"
timeflux test_2.yaml

echo "\n* Two graphs, one node each, interrupt with Ctrl+C"
timeflux test_3.yaml

echo "\n* Two graphs, one node each, interrupt with exception"
timeflux test_4.yaml

echo "\n* Two graphs, one node each, interrupt during initialization"
timeflux test_5.yaml

echo "\n* Two graphs, one node each, interrupt during initialization"
timeflux test_6.yaml

echo "\n* Two graphs, one node each, interrupt with unhandled exception in update"
timeflux test_7.yaml

echo "\n* Two graphs, one node each, interrupt with unhandled exception in init"
timeflux test_8.yaml

echo "\n* Two graphs, one node each, interrupt with unhandled exception in post hook"
export TIMEFLUX_HOOK_POST=post_exception
timeflux test_9.yaml