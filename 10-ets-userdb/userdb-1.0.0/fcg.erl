%
% Functions call generator - zatim jenl linearne jedna po druhe
%
-module(fcg).

-export ([run/4, run/3]).
-export ([run_calls/4, run_calls/3]).

run (Module, Function, Params, N) when N > 0 ->
	{RunTime, _Result} = timer:tc(?MODULE, run_calls, [Module, Function, Params, N]),
	out_result (RunTime, N).

run (Fun, Params, N) when N > 0 ->
	{RunTime, _Result} = timer:tc(?MODULE, run_calls, [Fun, Params, N]),
	out_result (RunTime, N).

out_result (RunTime, N) ->
	io:format("runtime: ~w Sec, one request: ~w uSec~n", [RunTime/1000000, RunTime/N]).

run_calls(_Module, _Function, _Params, 0) ->
	ok;
run_calls(Module, Function, Params, N) when N>0 ->
	_Result = apply(Module, Function, Params),
	run_calls(Module, Function, Params, N - 1).

run_calls(_Fun, _Params, 0) ->
	ok;
run_calls(Fun, Params, N) when N>0 ->
	_Result = apply(Fun, Params),
	run_calls(Fun, Params, N - 1).
