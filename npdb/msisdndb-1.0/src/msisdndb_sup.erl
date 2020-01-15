%%
%% main supervisor module
%%

-module(msisdndb_sup).
-behavior(supervisor).

-export([start_link/1]).
-export([start_loader/0, stop_loader/0]).
-export([init/1]).

start_link (SupArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, SupArgs).

start_loader () ->
    Loader = {
        msisdndb_loader, {msisdndb_tab_loader_sup, start_link, []},
        permanent, 2000, supervisor, [msisdndb_tab_loader_sup]
    },
    supervisor:start_child(?MODULE, Loader).

stop_loader () ->
    case supervisor:terminate_child(?MODULE, msisdndb_loader) of
        ok -> supervisor:delete_child (?MODULE, msisdndb_loader);
        {error, Reason} -> {error, Reason}
    end.

init (_SupArgs) ->

    Stats = {
        msisdndb_stats, {msisdndb_stats, start_link, []},
        permanent, 2000, worker, [msisdndb_stats]
    },

    RdcDb = {
        msisdndb_rdc, {msisdndb_rdc, start_link, []},
        permanent, 2000, worker, [msisdndb_rdc]
    },

    FixDb = {
        msisdndb_npfix, {msisdndb_npfix, start_link, []},
        permanent, 2000, worker, [msisdndb_npfix]
    },

    MobDb = {
        msisdndb_npmob, {msisdndb_npmob, start_link, []},
        permanent, 2000, worker, [msisdndb_npmob]
    },

    ExtraDb = {
        msisdndb_extra, {msisdndb_extra, start_link, []},
        permanent, 2000, worker, [msisdndb_extra]
    },

    {ok, {{one_for_one, 1000, 1}, [Stats, RdcDb, FixDb, MobDb, ExtraDb]}}.
