%%% ===========================================================================
%%%  Application module
%%% ===========================================================================
-module(msisdndb_app).
-behavior(application).
-export ([start/2, stop/1]).

start(_Type, _AppArgs) ->
    msisdndb_sup:start_link([]).

stop(_State) ->
	ok.
