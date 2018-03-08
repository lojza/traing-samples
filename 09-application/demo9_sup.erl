%% ============================================================================
%%  supervisor demo
%% ============================================================================
-module(demo9_sup).
-behavior(supervisor).

-export([start_link/1, stop/0]).
-export([init/1]).

start_link (StartArgs) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, StartArgs).

init (_StartArgs) ->
    Worker  = {
        worker,
        {demo9_worker, start_link, []}, 
	permanent, 500, worker, [demo9_worker]
    },

    {ok, {{one_for_all, 1, 1}, [Worker]}}.

% Supervisor has no stop function.
% This can emulate it.
stop() -> exit(whereis(?MODULE), shutdown).
