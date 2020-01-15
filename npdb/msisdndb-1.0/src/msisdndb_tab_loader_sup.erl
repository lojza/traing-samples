%% ============================================================================
%%  Data loader supervisor
%% ============================================================================

-module(msisdndb_tab_loader_sup).
-behavior(supervisor).

-export([start_link/0, start_link/1, stop/0]).
-export([load_info/0]).
-export([init/1]).

-include("msisdndb_logging.hrl").

%% ----------------------------------------------------------------------------
%%  O & M functions
%% ----------------------------------------------------------------------------

start_link () ->
    Opts = case application:get_env(input_base_path) of
        {ok, InputBasePath} ->
            O = [{load_file_info_path, InputBasePath},
                 {input_mob_full, filename:join([InputBasePath,"mob", "full"])},
                 {input_mob_incr, filename:join([InputBasePath,"mob", "incr"])},
                 {input_fix_full, filename:join([InputBasePath,"fix", "full"])},
                 {input_fix_incr, filename:join([InputBasePath,"fix", "incr"])},
                 {input_rdc_full, filename:join([InputBasePath,"rdc", "full"])},
                 {input_extra_full, filename:join([InputBasePath,"extra", "full"])}],

            ?LOG_INFO ("Using input file paths: ~p", [O]),
            O;
        undefined -> []
    end,
    start_link (Opts).

start_link (Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Opts).

stop() ->
    exit(whereis(?MODULE), kill).

load_info () ->

  [{Id, msisdndb_tab_loader:load_info(ProcName)} || {Id, ProcName} <- 
    [ {npmob, msisdndb_loader_mob},
      {npfix, msisdndb_loader_fix},
      {rdc,   msisdndb_loader_rdc},
      {extra, msisdndb_loader_extra} ]].

%% ----------------------------------------------------------------------------
%%  supervisor callback
%% ----------------------------------------------------------------------------

init (Opts) ->

    Mob = {
        loader_mob, {msisdndb_tab_loader, start_link, [msisdndb_loader_mob, npmob, Opts]},
        permanent, 2000, worker, [msisdndb_tab_loader]
    },

    Fix = {
        loader_fix, {msisdndb_tab_loader, start_link, [msisdndb_loader_fix, npfix, Opts]},
        permanent, 2000, worker, [msisdndb_tab_loader]
    },

    Rdc = {
        loader_rdc, {msisdndb_tab_loader, start_link, [msisdndb_loader_rdc, rdc, Opts]},
        permanent, 2000, worker, [msisdndb_tab_loader]
    },

    Extra = {
        loader_extra, {msisdndb_tab_loader, start_link, [msisdndb_loader_extra, extra, Opts]},
        permanent, 2000, worker, [msisdndb_tab_loader]
    },

    {ok, {{one_for_one, 1000, 1}, [Mob, Fix, Rdc, Extra]}}.
