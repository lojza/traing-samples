%% ============================================================================
%%  app demo
%% ============================================================================
-module(demo9_app).
-behavior(application).

-export ([start/2, stop/1]).

start(_Type, StartArgs) ->
    demo9_sup:start_link (StartArgs). 

stop(_State) ->
    ok. 
