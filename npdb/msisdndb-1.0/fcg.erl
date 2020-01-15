%%
%% function cal generator
%%

-module(fcg).

-export([run/4]).

% maun API
run(M,F,A,N) ->
   T1 = erlang:monotonic_time(),
   run_loop(M,F,A,N,N,T1).

% internal loop
run_loop(M,F,A,Tot,N,T1) when N > 0 ->
   apply(M,F,A),
   run_loop(M,F,A,Tot,N-1, T1);

run_loop(_,_,_,Tot,0, T1) ->
   T2 = erlang:monotonic_time(),
   USec = erlang:convert_time_unit(T2 - T1, native, microsecond),
   T = USec / Tot,
   % XXX: put here some report printing
   {Tot, USec, T}.
