% main supervisor

-module(userdb_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Db = { userdb, 
           {userdb, start_link, []},
           permanent, 2000,
           worker,
           [userdb]
         },

    {ok, { {one_for_one, 5, 10}, [Db]} }.
